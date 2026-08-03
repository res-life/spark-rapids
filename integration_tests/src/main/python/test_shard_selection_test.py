# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from types import SimpleNamespace

import pytest

import conftest


class _Hook:
    def __init__(self):
        self.deselected = []

    def pytest_deselected(self, items):
        self.deselected.extend(items)


class _PluginManager:
    @staticmethod
    def get_plugin(_name):
        return None


def _config():
    return SimpleNamespace(hook=_Hook(), pluginmanager=_PluginManager())


def _items(count=20):
    return [SimpleNamespace(nodeid=f"file_test.py::test_case[{index}]")
            for index in range(count)]


def test_java_string_hashcode_is_stable():
    assert conftest._java_string_hashcode("abc") == 96354
    assert conftest._java_string_hashcode("file.py::test_case[0]") == 152671142
    assert conftest._java_string_hashcode("😀") == 1772899
    assert conftest._java_string_hashcode("zzzzzz") == -685785664


def test_two_shards_are_disjoint_and_complete(monkeypatch):
    original = _items()
    selected = []

    for shard_index in range(2):
        items = list(original)
        config = _config()
        monkeypatch.setenv("TEST_SHARD_INDEX", str(shard_index))
        monkeypatch.setenv("TEST_SHARD_COUNT", "2")
        conftest._apply_test_shard(config, items)

        assert set(item.nodeid for item in items).isdisjoint(selected)
        assert {item.nodeid for item in config.hook.deselected} == (
            {item.nodeid for item in original} - {item.nodeid for item in items})
        selected.extend(item.nodeid for item in items)

    assert set(selected) == {item.nodeid for item in original}


@pytest.mark.parametrize(
    "shard_index,shard_count,error",
    [
        (None, "2", "must be set together"),
        ("0", None, "must be set together"),
        ("zero", "2", "must be integers"),
        ("0", "one", "must be integers"),
        ("0", "1", "must be at least 2"),
        ("-1", "2", "must be between"),
        ("2", "2", "must be between"),
    ])
def test_invalid_shard_config(monkeypatch, shard_index, shard_count, error):
    if shard_index is None:
        monkeypatch.delenv("TEST_SHARD_INDEX", raising=False)
    else:
        monkeypatch.setenv("TEST_SHARD_INDEX", shard_index)
    if shard_count is None:
        monkeypatch.delenv("TEST_SHARD_COUNT", raising=False)
    else:
        monkeypatch.setenv("TEST_SHARD_COUNT", shard_count)

    with pytest.raises(pytest.UsageError, match=error):
        conftest._test_shard_config()
