import os

from load_workbook import load_workbooks
from prepare_data import prepare_twb_data
from parse_twb import create_json
import config


if __name__=='__main__':
    # load workbooks from Tableau server
    load_workbooks()
    # clean up data: remove extract files
    prepare_twb_data()
    # preparing final json
    result_json = []
    for twb_file in os.listdir(config.prepared_twb_dir):
        create_json(os.path.join(config.prepared_twb_dir, twb_file))
