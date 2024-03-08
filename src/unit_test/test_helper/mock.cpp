// Copyright(C) 2023 InfiniFlow, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module;

#include <filesystem>
#include <functional>
#include <regex>
#include <string>
#include <stdexcept>

module infinity_mock;

import stl;
import config;
import buffer_manager;
import default_values;
import wal_manager;
import catalog;
import catalog_delta_entry;
import txn_manager;
import builtin_functions;
import local_file_system;
import third_party;
import logger;

import txn;
import infinity_exception;
import status;
import background_process;
import status;
import bg_task;
import periodic_trigger_thread;
import periodic_trigger;

namespace infinity {

UniquePtr<MockCatalog> MockCatalog::NewCatalog(SharedPtr<String> dir, bool create_default_db) {
    auto catalog = MakeUnique<MockCatalog>(dir);
    if (create_default_db) {
        // db current dir is same level as catalog
        Path catalog_path(*catalog->current_dir_);
        Path parent_path = catalog_path.parent_path();
        auto data_dir = MakeShared<String>(parent_path.string());
        UniquePtr<DBMeta> db_meta = MakeUnique<DBMeta>(data_dir, MakeShared<String>("default"));
        UniquePtr<DBEntry> db_entry = MakeUnique<DBEntry>(db_meta.get(), false, db_meta->data_dir(), db_meta->db_name(), 0, 0);
        // TODO commit ts == 0 is true??
        db_entry->commit_ts_ = 0;
        db_meta->db_entry_list().emplace_front(std::move(db_entry));

        catalog->db_meta_map()["default"] = std::move(db_meta);
    }
    return catalog;
}

UniquePtr<MockCatalog> MockCatalog::LoadFromFiles(const Vector<String> &catalog_paths, BufferManager *buffer_mgr) {
    auto catalog = MakeUnique<MockCatalog>(nullptr);
    if (catalog_paths.empty()) {
        UnrecoverableError(fmt::format("Catalog path is empty"));
    }
    // 1. load json
    // 2. load entries
    LOG_INFO(fmt::format("Load base FULL catalog json from: {}", catalog_paths[0]));
    catalog = MockCatalog::LoadFromFile(catalog_paths[0], buffer_mgr);

    // Load catalogs delta checkpoints and merge.
    for (SizeT i = 1; i < catalog_paths.size(); i++) {
        LOG_INFO(fmt::format("Load catalog DELTA entry binary from: {}", catalog_paths[i]));
        Catalog::LoadFromEntry(catalog.get(), catalog_paths[i], buffer_mgr);
    }

    LOG_TRACE(fmt::format("Catalog Delta Op is done"));

    return catalog;
}

UniquePtr<MockCatalog> MockCatalog::LoadFromFile(const String &catalog_path, BufferManager *buffer_mgr) {
    UniquePtr<MockCatalog> catalog = nullptr;
    LocalFileSystem fs;
    UniquePtr<FileHandler> catalog_file_handler = fs.OpenFile(catalog_path, FileFlags::READ_FLAG, FileLockType::kReadLock);
    SizeT file_size = fs.GetFileSize(*catalog_file_handler);
    String json_str(file_size, 0);
    SizeT n_bytes = catalog_file_handler->Read(json_str.data(), file_size);
    if (file_size != n_bytes) {
        RecoverableError(Status::CatalogCorrupted(catalog_path));
    }

    nlohmann::json catalog_json = nlohmann::json::parse(json_str);
    Deserialize(catalog_json, buffer_mgr, catalog);
    return catalog;
}

void MockCatalog::Deserialize(const nlohmann::json &catalog_json, BufferManager *buffer_mgr, UniquePtr<MockCatalog> &catalog) {
    SharedPtr<String> current_dir = MakeShared<String>(catalog_json["current_dir"]);

    // FIXME: new catalog need a scheduler, current we use nullptr to represent it.
    catalog = MakeUnique<MockCatalog>(current_dir);
    catalog->next_txn_id_ = catalog_json["next_txn_id"];
    catalog->catalog_version_ = catalog_json["catalog_version"];
    if (catalog_json.contains("databases")) {
        for (const auto &db_json : catalog_json["databases"]) {
            UniquePtr<DBMeta> db_meta = DBMeta::Deserialize(db_json, buffer_mgr);
            catalog->db_meta_map().emplace(*db_meta->db_name(), std::move(db_meta));
        }
    }
}

bool MockCatalog::FlushGlobalCatalogDeltaEntry(const String &delta_catalog_path, TxnTimeStamp max_commit_ts, bool is_full_checkpoint) {
    LOG_INFO("FLUSH GLOBAL DELTA CATALOG ENTRY");
    LOG_INFO(fmt::format("Global catalog delta entry commit ts:{}, checkpoint max commit ts:{}.",
                         global_catalog_delta_entry_->commit_ts(),
                         max_commit_ts));

    // Check the SegmentEntry's for flush the data to disk.
    UniquePtr<CatalogDeltaEntry> flush_delta_entry = global_catalog_delta_entry_->PickFlushEntry(max_commit_ts);

    if (flush_delta_entry->operations().empty()) {
        LOG_INFO("Global catalog delta entry ops is empty. Skip flush.");
        return true;
    }
    Vector<TransactionID> flushed_txn_ids;
    for (auto &op : flush_delta_entry->operations()) {
        switch (op->GetType()) {
            case CatalogDeltaOpType::ADD_TABLE_ENTRY: {
                auto add_table_entry_op = static_cast<AddTableEntryOp *>(op.get());
                add_table_entry_op->SaveState();
                break;
            }
            case CatalogDeltaOpType::ADD_SEGMENT_ENTRY: {
                RecoverableError(Status::NotSupport("ExtractYear function isn't implemented"));
            }
            case CatalogDeltaOpType::ADD_SEGMENT_INDEX_ENTRY: {
                auto add_segment_index_entry_op = static_cast<AddSegmentIndexEntryOp *>(op.get());
                LOG_TRACE(fmt::format("Flush segment index entry: {}", add_segment_index_entry_op->ToString()));
                add_segment_index_entry_op->Flush(max_commit_ts);
                break;
            }
            default:
                break;
        }
        flushed_txn_ids.push_back(op->txn_id());
    }

    // Save the global catalog delta entry to disk.
    auto exp_size = flush_delta_entry->GetSizeInBytes();
    Vector<char> buf(exp_size);
    char *ptr = buf.data();
    flush_delta_entry->WriteAdv(ptr);
    i32 act_size = ptr - buf.data();
    if (exp_size != act_size) {
        UnrecoverableError(fmt::format("Flush global catalog delta entry failed, exp_size: {}, act_size: {}", exp_size, act_size));
    }

    std::ofstream outfile;
    outfile.open(delta_catalog_path, std::ios::binary);
    outfile.write((reinterpret_cast<const char *>(buf.data())), act_size);
    outfile.close();

    LOG_INFO(fmt::format("Flush global catalog delta entry to: {}, size: {}.", delta_catalog_path, act_size));

    if (!is_full_checkpoint) {
        Vector<TransactionID> flushed_txn_ids;
        for (auto &op : flush_delta_entry->operations()) {
            flushed_txn_ids.push_back(op->txn_id());
        }
        txn_mgr_->RemoveWaitFlushTxns(flushed_txn_ids);
    }

    return false;
}

void MockCatalog::simulateCrash() const {
    LOG_WARN("simulate crash");
    return;
}

void MockStorage::Init() {
    // Check the data dir to get latest catalog file.
    String catalog_dir = String(*config_ptr_->data_dir());
    catalog_dir.append("/");
    catalog_dir.append(CATALOG_FILE_DIR);

    // Construct buffer manager
    buffer_mgr_ = MakeUnique<BufferManager>(config_ptr_->buffer_pool_size(), config_ptr_->data_dir(), config_ptr_->temp_dir());

    // Construct wal manager
    wal_mgr_ = MakeUnique<MockWalManager>(this,
                                          Path(*config_ptr_->wal_dir()) / WAL_FILE_TEMP_FILE,
                                          config_ptr_->wal_size_threshold(),
                                          config_ptr_->full_checkpoint_interval_sec(),
                                          config_ptr_->delta_checkpoint_interval_sec(),
                                          config_ptr_->delta_checkpoint_interval_wal_bytes());

    // Must init catalog before txn manager.
    // Replay wal file wrap init catalog
    TxnTimeStamp system_start_ts = wal_mgr_->ReplayWalFile();

    bg_processor_ = MakeUnique<BGTaskProcessor>(wal_mgr_.get());
    // Construct txn manager
    txn_mgr_ = MakeUnique<TxnManager>(new_catalog_.get(),
                                      buffer_mgr_.get(),
                                      bg_processor_.get(),
                                      std::bind(&WalManager::PutEntry, wal_mgr_.get(), std::placeholders::_1),
                                      new_catalog_->next_txn_id_,
                                      system_start_ts);

    txn_mgr_->Start();
    // start WalManager after TxnManager since it depends on TxnManager.
    wal_mgr_->Start();

    bg_processor_->Start();

    BuiltinFunctions builtin_functions(new_catalog_);
    builtin_functions.Init();

    auto txn = txn_mgr_->CreateTxn();
    txn->Begin();
    SharedPtr<ForceCheckpointTask> force_ckp_task = MakeShared<ForceCheckpointTask>(txn);
    bg_processor_->Submit(force_ckp_task);
    force_ckp_task->Wait();
    txn_mgr_->CommitTxn(txn);

    {
        periodic_trigger_thread_ = MakeUnique<PeriodicTriggerThread>();

        std::chrono::seconds cleanup_interval = config_ptr_->cleanup_interval();
        if (cleanup_interval.count() > 0) {
            periodic_trigger_thread_->AddTrigger(
                MakeUnique<CleanupPeriodicTrigger>(cleanup_interval, bg_processor_.get(), new_catalog_.get(), txn_mgr_.get()));
        } else {
            LOG_WARN("Cleanup interval is not set, auto cleanup task will not be triggered");
        }

        periodic_trigger_thread_->Start();
    }
}

void MockStorage::UnInit() {
    fmt::print("Shutdown storage ...\n");
    periodic_trigger_thread_->Stop();
    bg_processor_->Stop();

    wal_mgr_->Stop();

    txn_mgr_.reset();
    bg_processor_.reset();
    wal_mgr_.reset();

    // Buffer Manager need to be destroyed before catalog. since buffer manage hold the raw pointer owned by catalog:
    // such as index definition and index base of IndexFileWorker
    buffer_mgr_.reset();
    new_catalog_.reset();

    config_ptr_ = nullptr;
    fmt::print("Shutdown storage successfully\n");
}

void MockStorage::AttachCatalog(const Vector<String> &catalog_files) {
    LOG_INFO(fmt::format("Attach catalogs from {} files", catalog_files.size()));
    for (const auto &catalog_file : catalog_files) {
        LOG_TRACE(fmt::format("Catalog file: {}", catalog_file.c_str()));
    }
    new_catalog_ = MockCatalog::LoadFromFiles(catalog_files, buffer_mgr_.get());
}

void MockStorage::InitNewCatalog() {
    LOG_INFO("Init new catalog");
    String catalog_dir = String(*config_ptr_->data_dir());
    catalog_dir.append("/");
    catalog_dir.append(CATALOG_FILE_DIR);
    LocalFileSystem fs;
    if (!fs.Exists(catalog_dir)) {
        fs.CreateDirectory(catalog_dir);
    }
    new_catalog_ = MockCatalog::NewCatalog(MakeShared<String>(catalog_dir), true);
}

} // namespace infinity
