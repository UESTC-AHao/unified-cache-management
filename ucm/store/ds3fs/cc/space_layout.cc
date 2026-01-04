/**
 * MIT License
 *
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
#include "space_layout.h"
#include <algorithm>
#include <fmt/ranges.h>
#include "ds3fs_file.h"
#include "logger/logger.h"

namespace UC::Ds3fsStore {

Status SpaceLayout::Setup(const std::vector<std::string>& storageBackends)
{
    if (storageBackends.empty()) {
        UC_ERROR("Empty backend list.");
        return Status::InvalidParam();
    }
    auto status = Status::OK();
    for (auto& path : storageBackends) {
        if ((status = this->AddStorageBackend(path)).Failure()) { return status; }
    }
    return status;
}

std::string SpaceLayout::DataFilePath(const Detail::BlockId& blockId, bool activated) const
{
    const auto& backend = StorageBackend(blockId);
    const auto& file = DataFileName(blockId);
    const auto& parent = DataParentName(file, activated);
    return fmt::format("{}{}/{}", backend, parent, file);
}

Status SpaceLayout::CommitFile(const Detail::BlockId& blockId, bool success) const
{
    const auto& backend = StorageBackend(blockId);
    const auto& file = DataFileName(blockId);
    const auto& activated = fmt::format("{}{}/{}", backend, TempFileRoot(), file);
    auto s = Status::OK();
    if (success) {
        const auto& parent = fmt::format("{}{}", backend, DataParentName(file, false));
        const auto& archived = fmt::format("{}/{}", parent, file);
        Ds3fsFile dir(parent);
        s = dir.MkDir();
        if (s == Status::OK() || s == Status::DuplicateKey()) {
            Ds3fsFile activatedFile(activated);
            s = activatedFile.Rename(archived);
        }
    }
    if (!success || s.Failure()) {
        Ds3fsFile activatedFile(activated);
        activatedFile.Remove();
    }
    return s;
}

std::vector<std::string> SpaceLayout::RelativeRoots() const { return {TempFileRoot()}; }

Status SpaceLayout::AddStorageBackend(const std::string& path)
{
    auto normalizedPath = path;
    if (normalizedPath.back() != '/') { normalizedPath += '/'; }
    auto status = Status::OK();
    if (this->storageBackends_.empty()) {
        status = this->AddFirstStorageBackend(normalizedPath);
    } else {
        status = this->AddSecondaryStorageBackend(normalizedPath);
    }
    if (status.Failure()) {
        UC_ERROR("Failed({}) to add storage backend({}).", status, normalizedPath);
    }
    return status;
}

Status SpaceLayout::AddFirstStorageBackend(const std::string& path)
{
    for (const auto& root : this->RelativeRoots()) {
        Ds3fsFile dir(path + root);
        auto status = dir.MkDir();
        if (status == Status::DuplicateKey()) { status = Status::OK(); }
        if (status.Failure()) { return status; }
    }
    this->storageBackends_.emplace_back(path);
    return Status::OK();
}

Status SpaceLayout::AddSecondaryStorageBackend(const std::string& path)
{
    auto iter = std::find(this->storageBackends_.begin(), this->storageBackends_.end(), path);
    if (iter != this->storageBackends_.end()) { return Status::OK(); }
    constexpr auto accessMode = Ds3fsFile::AccessMode::READ | Ds3fsFile::AccessMode::WRITE;
    for (const auto& root : this->RelativeRoots()) {
        Ds3fsFile dir(path + root);
        auto status = dir.Access(accessMode);
        if (status.Failure()) { return status; }
    }
    this->storageBackends_.emplace_back(path);
    return Status::OK();
}

std::string SpaceLayout::StorageBackend(const Detail::BlockId& blockId) const
{
    static std::hash<std::string> hasher;
    const auto size = this->storageBackends_.size();
    std::string blockIdStr = fmt::format("{:02x}", fmt::join(blockId, ""));
    if (size == 1) { return storageBackends_.front(); }
    return this->storageBackends_[hasher(blockIdStr) % size];
}

std::string SpaceLayout::DataParentName(const std::string& blockFile, bool activated) const
{
    if (activated) { return TempFileRoot(); }
    return blockFile.substr(0, 2);
}

std::string SpaceLayout::DataFileRoot() const { return "data"; }

std::string SpaceLayout::TempFileRoot() const { return ".temp"; }

std::string SpaceLayout::DataFileName(const Detail::BlockId& blockId) const
{
    return fmt::format("{:02x}", fmt::join(blockId, ""));
}

}  // namespace UC::Ds3fsStore