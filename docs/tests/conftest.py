import os
from pathlib import Path
import pytest
import sys

os.path.abspath(__file__)
test_root = Path(os.path.dirname(os.path.abspath(__file__)))
root = Path(test_root.parent / "etl").absolute()
sys.path.append(str(root))
sys.path.append(str(test_root))


@pytest.fixture(scope="module")
def exp_parse_conf():
    return {
        "ch10_packet_type": {
            "MILSTD1553_FORMAT1": True,
            "VIDEO_FORMAT0": True,
            "ETHERNET_DATA0": True,
        },
        "parse_chunk_bytes": 200,
        "parse_thread_count": 2,
        "max_chunk_read_count": 1000,
        "worker_offset_wait_ms": 200,
        "worker_shift_wait_ms": 200,
    }


@pytest.fixture(scope="module")
def exp_modified_parse_conf():
    return {
        "ch10_packet_type": {
            "MILSTD1553_FORMAT1": True,
            "VIDEO_FORMAT0": False,
            "ETHERNET_DATA0": True,
        },
        "parse_chunk_bytes": 200,
        "parse_thread_count": 20,
        "max_chunk_read_count": 3000,
        "worker_offset_wait_ms": 200,
        "worker_shift_wait_ms": 200,
    }


@pytest.fixture(scope="module")
def exp_trans_conf():
    return {
        "translate_thread_count": 1,
        "use_tmats_busmap": False,
        "tmats_busname_corrections": {},
        "prompt_user": False,
        "stop_after_bus_map": False,
        "vote_threshold": 1,
        "vote_method_checks_tmats": False,
        "bus_name_exclusions": [],
        "select_specific_messages": [],
        "exit_after_table_creation": False,
        "auto_sys_limits": True,
    }


@pytest.fixture(scope="module")
def exp_modified_trans_conf():
    return {
        "translate_thread_count": 3,
        "use_tmats_busmap": False,
        "tmats_busname_corrections": {},
        "prompt_user": True,
        "stop_after_bus_map": False,
        "vote_threshold": 5,
        "vote_method_checks_tmats": False,
        "bus_name_exclusions": ["test"],
        "select_specific_messages": [],
        "exit_after_table_creation": False,
        "auto_sys_limits": True,
    }


@pytest.fixture(scope="module")
def exp_parse_conf_schema():
    return {
        "ch10_packet_type": {"MILSTD1553_FORMAT1": "BOOL", "VIDEO_FORMAT0": "BOOL"},
        "parse_chunk_bytes": "INT",
        "parse_thread_count": "INT",
        "max_chunk_read_count": "INT",
        "worker_offset_wait_ms": "INT",
        "worker_shift_wait_ms": "INT",
    }


@pytest.fixture(scope="module")
def exp_trans_conf_schema():
    return {
        "translate_thread_count": "INT",
        "use_tmats_busmap": "BOOL",
        "tmats_busname_corrections": {"_NOT_DEFINED_OPT_": "STR"},
        "prompt_user": "BOOL",
        "stop_after_bus_map": "BOOL",
        "vote_threshold": "INT",
        "vote_method_checks_tmats": "BOOL",
        "bus_name_exclusions": ["OPTSTR"],
        "select_specific_messages": ["OPTSTR"],
        "exit_after_table_creation": "BOOL",
        "auto_sys_limits": "BOOL",
    }


@pytest.fixture(scope="module")
def exp_dts_schema():
    return {
        "translatable_message_definitions": {
            "_NOT_DEFINED_": {
                "msg_data": {
                    "command": ["INT"],
                    "lru_addr": ["INT"],
                    "lru_subaddr": ["INT"],
                    "lru_name": ["OPTSTR"],
                    "bus": "STR",
                    "wrdcnt": "INT",
                    "rate": "FLT",
                    "mode_code": "OPTBOOL",
                    "desc": "OPTSTR",
                },
                "word_elem": {
                    "_NOT_DEFINED_OPT_": {
                        "off": "INT",
                        "cnt": "INT",
                        "schema": "STR-->{SIGNED16,SIGNED32,UNSIGNED16,UNSIGNED32,FLOAT32_1750,FLOAT32_IEEE,FLOAT64_IEEE,FLOAT16,CAPS,FLOAT32_GPS,FLOAT64_GPS}",
                        "msbval": "FLT",
                        "desc": "OPTSTR",
                        "uom": "OPTSTR",
                        "multifmt": "OPTBOOL",
                        "class": "OPTINT",
                    }
                },
                "bit_elem": {
                    "_NOT_DEFINED_OPT_": {
                        "off": "INT",
                        "cnt": "INT",
                        "schema": "STR-->{UNSIGNEDBITS,SIGNEDBITS,ASCII}",
                        "msbval": "FLT",
                        "desc": "OPTSTR",
                        "uom": "OPTSTR",
                        "multifmt": "OPTBOOL",
                        "class": "OPTINT",
                        "msb": "INT",
                        "lsb": "INT",
                        "bitcnt": "INT",
                    }
                },
            }
        },
        "supplemental_bus_map_command_words": {"_NOT_DEFINED_OPT_": [["INT"]]},
    }
