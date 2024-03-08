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

// #include "../../third_party/googletest/googlemock/include/gmock/gmock.h"

export module infinity_mock;

import stl;
import logger;
import config;
import resource_manager;
import task_scheduler;
import storage;
import singleton;
import session_manager;

import catalog;
import txn_manager;
import buffer_manager;
import wal_manager;
import background_process;
import periodic_trigger_thread;

import table_def;
import function;
import function_set;
import table_function;
import special_function;
import third_party;
import profiler;
import status;
import default_values;
import table_detail;
import index_base;
import txn_store;
import data_access_state;
import extra_ddl_info;
import db_entry;
import table_entry;
import table_index_entry;
import segment_entry;
import db_meta;
import meta_map;
import base_entry;
import meta_entry_interface;
import cleanup_scanner;

import builtin_functions;
import local_file_system;

import txn;
import infinity_exception;
import status;
import background_process;
import bg_task;
import periodic_trigger;
import wal_entry;
import local_file_system;
import file_system_type;
import file_system;

namespace infinity {

export class MockStorage : public Storage {
public:
    // TODO: add other mock options
    explicit MockStorage(const Config *config_ptr, bool mock_catalog) : Storage(config_ptr), mock_catalog_(mock_catalog) {}

    void Init() override;

    void UnInit() override;

    void AttachCatalog(const Vector<String> &catalog_paths) override;

    void InitNewCatalog() override;

private:
    bool mock_catalog_{false};
};

export class MockInfinityContext : public Singleton<MockInfinityContext> {
public:
    [[nodiscard]] inline TaskScheduler *task_scheduler() noexcept { return task_scheduler_.get(); }

    [[nodiscard]] inline Config *config() noexcept { return config_.get(); }

    [[nodiscard]] inline Storage *storage() noexcept { return storage_.get(); }

    [[nodiscard]] inline ResourceManager *resource_manager() noexcept { return resource_manager_.get(); }

    [[nodiscard]] inline SessionManager *session_manager() noexcept { return session_mgr_.get(); }

    void Init(const SharedPtr<String> &config_path);

    void
    Init(const SharedPtr<String> &config_path, bool mock_resource_manager, bool mock_task_scheduler, bool mock_storage, bool mock_session_manager);

    void UnInit();

private:
    friend class Singleton;

    MockInfinityContext() = default;

    UniquePtr<Config> config_{};
    UniquePtr<ResourceManager> resource_manager_{};
    UniquePtr<TaskScheduler> task_scheduler_{};
    UniquePtr<Storage> storage_{};
    UniquePtr<SessionManager> session_mgr_{};

    bool initialized_{false};
};

void MockInfinityContext::Init(const SharedPtr<String> &config_path) {
    if (initialized_) {
        return;
    } else {
        // Config
        config_ = MakeUnique<Config>();
        config_->Init(config_path);

        Logger::Initialize(config_.get());

        resource_manager_ = MakeUnique<ResourceManager>(config_->worker_cpu_limit(), config_->total_memory_size());

        task_scheduler_ = MakeUnique<TaskScheduler>(config_.get());

        session_mgr_ = MakeUnique<SessionManager>();

        storage_ = MakeUnique<Storage>(config_.get());
        storage_->Init();

        initialized_ = true;
    }
}

void MockInfinityContext::Init(const SharedPtr<String> &config_path,
                               bool mock_resource_manager,
                               bool mock_task_scheduler,
                               bool mock_storage,
                               bool mock_session_manager) {
    if (initialized_) {
        return;
    } else {
        // Config
        config_ = MakeUnique<Config>();
        config_->Init(config_path);

        Logger::Initialize(config_.get());

        if (mock_resource_manager) {
        } else {
            resource_manager_ = MakeUnique<ResourceManager>(config_->worker_cpu_limit(), config_->total_memory_size());
        }

        if (mock_task_scheduler) {
        } else {
            task_scheduler_ = MakeUnique<TaskScheduler>(config_.get());
        }

        if (mock_session_manager) {
        } else {
            session_mgr_ = MakeUnique<SessionManager>();
        }

        if (mock_storage) {
            storage_ = MakeUnique<MockStorage>(config_.get(), true);
            storage_->Init();
        } else {
            storage_ = MakeUnique<Storage>(config_.get());
            storage_->Init();
        }

        initialized_ = true;
    }
}

void MockInfinityContext::UnInit() {
    if (!initialized_) {
        return;
    }
    initialized_ = false;

    storage_->UnInit();
    storage_.reset();

    session_mgr_.reset();

    task_scheduler_->UnInit();
    task_scheduler_.reset();

    resource_manager_.reset();

    Logger::Shutdown();

    config_.reset();
}

export class MockCatalog : public Catalog {
public:
    explicit MockCatalog(SharedPtr<String> dir) : Catalog(dir) {}

    static UniquePtr<MockCatalog> NewCatalog(SharedPtr<String> dir, bool create_default_db);

    static UniquePtr<MockCatalog> LoadFromFiles(const Vector<String> &catalog_paths, BufferManager *buffer_mgr);

    static UniquePtr<MockCatalog> LoadFromFile(const String &catalog_path, BufferManager *buffer_mgr);

    static void Deserialize(const nlohmann::json &catalog_json, BufferManager *buffer_mgr, UniquePtr<MockCatalog> &catalog);

    bool FlushGlobalCatalogDeltaEntry(const String &delta_catalog_path, TxnTimeStamp max_commit_ts, bool is_full_checkpoint) override;

    // MOCK_METHOD3(FlushGlobalCatalogDeltaEntry, bool(const String &delta_catalog_path, TxnTimeStamp max_commit_ts, bool is_full_checkpoint));

    void simulateCrash() const;
};


export class MockWalManager : public WalManager {
public:
    MockWalManager(Storage *storage,
                   String wal_path,
                   u64 wal_size_threshold,
                   u64 full_checkpoint_interval_sec,
                   u64 delta_checkpoint_interval_sec,
                   u64 delta_checkpoint_interval_wal_bytes)
        : WalManager(storage,
                     wal_path,
                     wal_size_threshold,
                     full_checkpoint_interval_sec,
                     delta_checkpoint_interval_sec,
                     delta_checkpoint_interval_wal_bytes) {}

};

} // namespace infinity
