# coding:utf8

import os
import zipfile
import shutil

import config


def prepare_twb_data():
    buffer_dir = os.path.join(config.prepared_twb_dir, 'buffer')
    for twb_file in os.listdir(config.twb_path):
        os.mkdir(buffer_dir)
        file_path = os.path.join(config.twb_path, twb_file)
        # in case of zipped report (with exctracted data)
        if file_path[-5:] == '.twbx':
            file_name = twb_file[:-5]
            with zipfile.ZipFile(file_path, 'r') as zip_f:
                zip_f.extractall(buffer_dir)
                shutil.rmtree(os.path.join(buffer_dir, 'Data'))
                extracted_file_name = os.path.join(buffer_dir, os.listdir(buffer_dir)[0])
                shutil.copyfile(extracted_file_name, os.path.join(config.prepared_twb_dir, '{}.twb'.format(file_name)))
        elif file_path[-4:] == '.twb':
            file_name = twb_file[:-4]
            shutil.copyfile(os.path.join(config.twb_path, twb_file), os.path.join(config.prepared_twb_dir, file_name))
        shutil.rmtree(buffer_dir)


