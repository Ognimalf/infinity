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

import stl;
import config;
import catalog;
import txn_manager;
import buffer_manager;
import wal_manager;
import background_process;
import periodic_trigger_thread;

export module storage;

namespace infinity {

export class Storage {
public:
    explicit Storage(const Config *config_ptr);

    virtual ~Storage() {};

    [[nodiscard]] inline Catalog *catalog() noexcept { return new_catalog_.get(); }

    [[nodiscard]] inline BufferManager *buffer_manager() noexcept { return buffer_mgr_.get(); }

    [[nodiscard]] inline TxnManager *txn_manager() const noexcept { return txn_mgr_.get(); }

    [[nodiscard]] inline WalManager *wal_manager() const noexcept { return wal_mgr_.get(); }

    [[nodiscard]] inline BGTaskProcessor *bg_processor() const noexcept { return bg_processor_.get(); }

    virtual void Init();

    virtual void UnInit();

    virtual void AttachCatalog(const Vector<String> &catalog_files);

    virtual void InitNewCatalog();

protected:
    const Config *config_ptr_{};
    UniquePtr<Catalog> new_catalog_{};
    UniquePtr<BufferManager> buffer_mgr_{};
    UniquePtr<TxnManager> txn_mgr_{};
    UniquePtr<WalManager> wal_mgr_{};
    UniquePtr<BGTaskProcessor> bg_processor_{};
    UniquePtr<PeriodicTriggerThread> periodic_trigger_thread_{};
};

} // namespace infinity
