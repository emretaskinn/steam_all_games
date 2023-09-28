import unittest
from steam_all_appids import iterator
from tqdm import tqdm
import steam_all_appids
import json
import pandas as pd


class TestStringMethods(unittest.TestCase):

    def test_iterator(self):
        test_list = {'appid': [10, 20, 30, 40, 50, 60, 70, 80, 90, 130, 220, 240, 280, 300, 320, 340]}
        start = 0
        finish = 5
        step = 5
        n = int(len(test_list['appid'])/step)
        bar = tqdm(total=n)
        self.assertTrue(iterator(start, finish, step, test_list['appid'], bar))

    def test_push_to_bq(self):
        table_id = "nth-wording-258215.steam_all_games.steam_all_games_test"
        # - get len(datafrabe.index)
        initial_table_length = len(
            steam_all_appids.get_existing_steam_data_from_bq(table_id).index
        )
        print(initial_table_length)
        with open("game_data.json") as f:
            # - mock game data
            game_data = json.loads(f.read())
            # - prepare it
            game_data = pd.DataFrame.from_dict(game_data, orient='index')
            game_data = pd.json_normalize(game_data['data'])
            game_data = steam_all_appids.wrangle(game_data)
            print(game_data)
            # - push to bq
            steam_all_appids.push_to_bq(game_data, table_id)
            # - get current length
        updated_table_length = len(
            steam_all_appids.get_existing_steam_data_from_bq(table_id).index
        )
        print(updated_table_length)
        # - assertTrue new length is more than old length
        self.assertTrue(initial_table_length < updated_table_length)
        # - delete what was pushed

    # def test_push_to_bq_for_non_game(self):



if __name__ == '__main__':
    unittest.main()
