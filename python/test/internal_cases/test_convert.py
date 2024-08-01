import pytest
from infinity.errors import ErrorCode
from common import common_values
import infinity
from infinity.remote_thrift.query_builder import InfinityThriftQueryBuilder
from infinity.common import ConflictType, InfinityException
from http_adapter import http_adapter

@pytest.mark.usefixtures("local_infinity")
@pytest.mark.usefixtures("http")
class TestInfinity:
    @pytest.fixture(autouse=True)
    def setup(self, local_infinity, http):
        if local_infinity:
            self.uri = common_values.TEST_LOCAL_PATH
        else:
            self.uri = common_values.TEST_LOCAL_HOST
        self.infinity_obj = infinity.connect(self.uri)
        if http:
            self.infinity_obj = http_adapter()
        assert self.infinity_obj

    def teardown(self):
        res = self.infinity_obj.disconnect()
        assert res.error_code == ErrorCode.OK

    def test_to_pl(self):
        db_obj = self.infinity_obj.get_database("default_db")
        db_obj.drop_table("test_to_pl", ConflictType.Ignore)
        db_obj.create_table("test_to_pl", {
            "c1": {"type": "int"}, "c2": {"type": "float"}}, ConflictType.Error)

        table_obj = db_obj.get_table("test_to_pl")
        table_obj.insert([{"c1": 1, "c2": 2}])
        print()
        res = table_obj.output(["c1", "c2"]).to_pl()
        print(res)
        res = table_obj.output(["c1", "c1"]).to_pl()
        print(res)
        res = table_obj.output(["c1", "c2", "c1"]).to_pl()
        print(res)
        db_obj.drop_table("test_to_pl", ConflictType.Error)
    def test_to_pa(self):
        db_obj = self.infinity_obj.get_database("default_db")
        db_obj.drop_table("test_to_pa", ConflictType.Ignore)
        db_obj.create_table("test_to_pa", {
            "c1": {"type": "int"}, "c2": {"type": "float"}}, ConflictType.Error)

        table_obj = db_obj.get_table("test_to_pa")
        table_obj.insert([{"c1": 1, "c2": 2.0}])
        print()
        res = table_obj.output(["c1", "c2"]).to_arrow()
        print(res)
        res = table_obj.output(["c1", "c1"]).to_arrow()
        print(res)
        res = table_obj.output(["c1", "c2", "c1"]).to_arrow()
        print(res)
        db_obj.drop_table("test_to_pa", ConflictType.Error)
    def test_to_df(self):
        db_obj = self.infinity_obj.get_database("default_db")
        db_obj.drop_table("test_to_df", ConflictType.Ignore)
        db_obj.create_table("test_to_df", {
            "c1": {"type": "int"}, "c2": {"type": "float"}}, ConflictType.Error)

        table_obj = db_obj.get_table("test_to_df")
        table_obj.insert([{"c1": 1, "c2": 2.0}])
        print()
        res = table_obj.output(["c1", "c2"]).to_df()
        print(res)
        res = table_obj.output(["c1", "c1"]).to_df()
        print(res)
        res = table_obj.output(["c1", "c2", "c1"]).to_df()
        print(res)
        db_obj.drop_table("test_to_df", ConflictType.Error)

    @pytest.mark.usefixtures("skip_if_http")
    def test_without_output_select_list(self):
        # connect
        db_obj = self.infinity_obj.get_database("default_db")
        db_obj.drop_table("test_without_output_select_list", ConflictType.Ignore)
        table_obj = db_obj.create_table("test_without_output_select_list", {
            "c1": {"type": "int"}, "c2": {"type": "float"}}, ConflictType.Error)

        table_obj.insert([{"c1": 1, "c2": 2.0}])

        with pytest.raises(InfinityException) as e:
            insert_res_df = table_obj.output([]).to_df()
            insert_res_arrow = table_obj.output([]).to_arrow()
            insert_res_pl = table_obj.output([]).to_pl()
            print(insert_res_df, insert_res_arrow, insert_res_pl)

        assert e.type == infinity.common.InfinityException
        assert e.value.args[0] == ErrorCode.EMPTY_SELECT_FIELDS

        db_obj.drop_table("test_without_output_select_list", ConflictType.Error)

    @pytest.mark.usefixtures("skip_if_http")
    @pytest.mark.parametrize("condition_list", ["c1 > 0.1 and c2 < 3.0",
                                                "c1 > 0.1 and c2 < 1.0",
                                                "c1 < 0.1 and c2 < 1.0",
                                                "c1",
                                                "c1 = 0",
                                                "_row_id",
                                                "*"])
    def test_convert_test_with_valid_select_list_output(self, condition_list):
        # connect
        db_obj = self.infinity_obj.get_database("default_db")
        db_obj.drop_table("test_with_valid_select_list_output", ConflictType.Ignore)
        table_obj = db_obj.create_table("test_with_valid_select_list_output", {
            "c1": {"type": "int"}, "c2": {"type": "float"}}, ConflictType.Error)
        table_obj.insert([{"c1": 1, "c2": 2.0},
                          {"c1": 10, "c2": 2.0},
                          {"c1": 100, "c2": 2.0},
                          {"c1": 1000, "c2": 2.0},
                          {"c1": 10000, "c2": 2.0}])

        insert_res_df = table_obj.output(["c1", condition_list]).to_pl()
        print(insert_res_df)

        db_obj.drop_table("test_with_valid_select_list_output", ConflictType.Error)

    @pytest.mark.parametrize("condition_list", [pytest.param("c1 + 0.1 and c2 - 1.0", ),
                                                pytest.param("c1 * 0.1 and c2 / 1.0", ),
                                                pytest.param("c1 > 0.1 %@#$sf c2 < 1.0", ),
                                                ])
    def test_convert_test_with_invalid_select_list_output(self, condition_list):
        # connect
        db_obj = self.infinity_obj.get_database("default_db")
        db_obj.drop_table("test_with_invalid_select_list_output", ConflictType.Ignore)
        table_obj = db_obj.create_table("test_with_invalid_select_list_output", {
            "c1": {"type": "int"}, "c2": {"type": "float"}}, ConflictType.Error)
        table_obj.insert([{"c1": 1, "c2": 2.0},
                          {"c1": 10, "c2": 2.0},
                          {"c1": 100, "c2": 2.0},
                          {"c1": 1000, "c2": 2.0},
                          {"c1": 10000, "c2": 2.0}])

        with pytest.raises(Exception):
            insert_res_df = table_obj.output(["c1", condition_list]).to_pl()
            print(insert_res_df)

        db_obj.drop_table("test_with_invalid_select_list_output", ConflictType.Error)

    # skipped tests using InfinityThriftQueryBuilder which is incompatible with local infinity
    @pytest.mark.usefixtures("skip_if_http")
    @pytest.mark.parametrize("filter_list", [
        "c1 > 10",
        "c2 > 1",
        "c1 > 0.1 and c2 < 3.0",
        "c1 > 0.1 and c2 < 1.0",
        "c1 < 0.1 and c2 < 1.0",
        "c1 < 0.1 and c1 > 1.0",
        "c1 = 0",
    ])
    @pytest.mark.usefixtures("skip_if_local_infinity")
    def test_convert_test_output_with_valid_filter_function(self, filter_list):
        # connect
        db_obj = self.infinity_obj.get_database("default_db")
        db_obj.drop_table("test_output_with_valid_filter_function", ConflictType.Ignore)
        table_obj = db_obj.create_table("test_output_with_valid_filter_function", {
            "c1": {"type": "int"}, "c2": {"type": "float"}}, ConflictType.Error)
        table_obj.insert([{"c1": 1, "c2": 2.0},
                          {"c1": 10, "c2": 2.0},
                          {"c1": 100, "c2": 2.0},
                          {"c1": 1000, "c2": 2.0},
                          {"c1": 10000, "c2": 2.0}])
        # TODO add more filter function
        insert_res_df = InfinityThriftQueryBuilder(table_obj).output(["*"]).filter(filter_list).to_pl()
        print(str(insert_res_df))

        db_obj.drop_table("test_output_with_valid_filter_function", ConflictType.Error)

    @pytest.mark.usefixtures("skip_if_http")
    @pytest.mark.usefixtures("skip_if_local_infinity")
    @pytest.mark.parametrize("filter_list", [
        pytest.param("c1"),
        pytest.param("_row_id"),
        pytest.param("*"),
        pytest.param("#@$%@#f"),
        pytest.param("c1 + 0.1 and c2 - 1.0"),
        pytest.param("c1 * 0.1 and c2 / 1.0"),
        pytest.param("c1 > 0.1 %@#$sf c2 < 1.0"),
    ])
    def test_convert_test_output_with_invalid_filter_function(self, filter_list):
        # connect
        db_obj = self.infinity_obj.get_database("default_db")
        db_obj.drop_table("test_output_with_invalid_filter_function", ConflictType.Ignore)
        table_obj = db_obj.create_table("test_output_with_invalid_filter_function", {
            "c1": {"type": "int"}, "c2": {"type": "float"}}, ConflictType.Error)
        table_obj.insert([{"c1": 1, "c2": 2.0},
                          {"c1": 10, "c2": 2.0},
                          {"c1": 100, "c2": 2.0},
                          {"c1": 1000, "c2": 2.0},
                          {"c1": 10000, "c2": 2.0}])
        # TODO add more filter function
        with pytest.raises(Exception) as e:
            insert_res_df = InfinityThriftQueryBuilder(table_obj).output(["*"]).filter(filter_list).to_pl()
            print(str(insert_res_df))

        print(e.type)

        db_obj.drop_table("test_output_with_invalid_filter_function", ConflictType.Error)