# coding:utf8
"""
Documentation about tableau server
https://github.com/tableau/server-client-python/blob/master/samples/explore_workbook.py
https://tableau.github.io/server-client-python/docs/api-ref#workbooksdownload

pip install tableauserverclient
"""

import os
import zipfile
import re

import tableauserverclient as TSC

import config


def load_workbooks():
    """

    :return: 
    """
    # authorize user
    tableau_auth = TSC.TableauAuth(config.username, config.password)
    # connect to server
    server = TSC.Server(config.server_name)

    with server.auth.sign_in(tableau_auth):
        for wb in TSC.Pager(server.workbooks):
            try:
                path = server.workbooks.download(wb.id, filepath=config.twb_path)
                # из коробки не сохраняет кирилицу - даём нормальное имя и на всякий случай удаляем расширение,
                # т.к. не всегда оно сохраняется и заменяем слеши
                wb_name = re.sub(r'\.twbx?', '', str(wb.name.encode('utf-8'))).replace('/', '\\')
                os.rename(path, '{}{}'.format(config.twb_path, wb_name))
                # если отчёт скачался вместе с экстрактом - выпиливаем экстракт
                if zipfile.is_zipfile('{}{}'.format(config.twb_path, wb_name)):
                    os.rename('{}{}'.format(config.twb_path, wb_name), '{}{}.twbx'.format(config.twb_path, wb_name))
                else:
                    os.rename('{}{}'.format(config.twb_path, wb_name), '{}{}.twb'.format(config.twb_path, wb_name))
            except TSC.server.endpoint.exceptions.ServerResponseError:
                pass
