# coding:utf8
""" В качестве аргумента комендной строки скрипт получает имя .twb
    В stdout выводится сформированный json
    пример использования:

    >> python parse_xml.py example.twb > example.json
"""

from lxml import etree
import sys
import json
import re


class TableauParser:
    def __init__(self, file_name):
        self.xml = etree.XML(open(file_name, 'r').read())
        self.result = dict()
        self.DS_FIELD_SET = ['remote-alias', 'local-name', 'local-type', 'aggregation']

    def create_json(self):
        """ Генерим json
        Фуннкция возвращает длинный json со всеми атрибутами, которые удалось найти
        """
        # ссылка на отчёт
        self.result.update({'workbook_link': self.get_link()})
        self.get_datasources()
        self.get_worksheets()
        self.get_dashboards()
        return self.result

    def get_link(self):
        name = self.xml.findall('repository-location')[0]
        self.result.update({'report_link': name.get('derived-from')})
        return name.get('derived-from')

    def get_datasources(self):
        items = self.xml.findall('datasources')
        arr = []
        for source in items:
            for item in source.iter('datasource'):
                # параметры отчёта (которые пробрасываются в запрос) генерят для себя data source, но он неинформативен
                if item.get('name') == "Parameters":
                    continue
                res = {}
                res.update({'datasouces_specs': dict(item.items())})
                res.update({'query': [i.text for i in item.iter('relation') if i.text is not None]})
                res.update({
                    'connection_params': {
                        y.get('name'): dict([x.items() for x in y.iter('connection')][0])
                        for y in item.iter('named-connection')
                    }
                })
                all_extracts = item.findall('extract')
                # итерируемся по экстрактам
                for i in all_extracts:
                    res.update({'has_extract': i.get('enabled')})  # является ли data source экстрактом
                    res.update({'extract_update_time': [k.get('update-time') for k in i.iter('connection')][0]})
                    # набор полей определяем в конструкторе, поле DS_FIELD_SET
                    res.update({
                        'extract_fields': [{
                             l.tag: l.text if l.tag != 'local-name' else re.sub('[\[\]]', '', l.text)
                             for l in f.iterchildren()
                             if (len(str(l.text).strip()) > 0 and l.tag in self.DS_FIELD_SET)
                            }
                            for f in i.iter('metadata-record')]}
                    )
                arr.append(res)
        self.result.update({'datasources': arr})
        return arr

    def get_worksheets(self):
        items = self.xml.findall('worksheets')
        arr = None
        for source in items:
            arr = []
            for item in source.iter('worksheet'):
                res = {}
                res.update({'sheet_name': item.get('name')})
                # тут измерения и меры
                res.update({
                    'columns': {
                        i.get('datasource'): [
                            dict([
                                (k[0], re.sub('[\[\]]', '', k[1]))
                                for k in j.items()
                            ])
                            for j in i.iter('column')
                        ]
                        for i in item.iter('datasource-dependencies')
                    }
                })
                res.update({
                    'column_items':
                    {
                        i.get('datasource'): [
                            dict([
                                (
                                    k[0], re.sub('[\[\]]', '', k[1])
                                    if k[0] != 'name'
                                    else k[1].split(':')[1]
                                )
                                for k in j.items()
                            ])
                            for j in i.iter('column-instance')
                        ]
                        for i in item.iter('datasource-dependencies')}
                })
                columns = {
                    i.get('datasource'): [
                        dict(j.items()) for j in i.iter('column')
                    ]
                    for i in item.iter('datasource-dependencies')
                }
                calculations = {
                    i.get('datasource'): [{
                            j.get('caption'): dict(l.items()) for l in j.iter('calculation')
                        }
                        for j in i.iter('column')
                    ]
                    for i in item.iter('datasource-dependencies')
                }
                for i in calculations.keys():  # имя источника данных, сколько листов, столько и источников
                    for j in columns[i]:  # в каждом листе пробегаемся по списку колонок
                        colname = j.get('caption', None)
                        if colname is not None:  # находим имя колонки
                            for jj in calculations[i]:
                                if len(jj) > 0:
                                    formula = jj.get(colname, None)  # достаём ф-лу, соответствующую листу + имя колонки
                                    if formula is not None:
                                        j.update(formula)
                # тут фильтры
                res.update({
                    'filters': [
                        dict([(
                                k[0], k[1]
                                if k[0] != 'column'
                                else k[1].split(':')[1]
                            )
                            for k in i.items()]
                        )
                        for i in item.iter('filter')
                    ]
                })
                arr.append(res)
        self.result.update({'worksheets': arr})

    def get_dashboards(self):
        items = self.xml.findall('dashboards')
        arr = []
        for source in items:
            for item in source.iter('dashboard'):
                res = {}
                res.update({'source_name': item.get('name')})
                # где-то тут фильтры
                res.update({
                    'columns': {
                        i.get('datasource'): [
                            dict([
                                (k[0], re.sub('[\[\]]', '', k[1]))
                                for k in j.items()
                            ])
                            for j in i.iter('column')
                        ]
                        for i in item.iter('datasource-dependencies')
                    }
                })
                res.update({'column_items': {
                    i.get('datasource'): [
                        dict([(
                            k[0],
                            re.sub('[\[\]]', '', k[1]) if k[0] != 'name' else k[1].split(':')[1]
                            )
                            for k in j.items()
                        ])
                        for j in i.iter('column-instance')]
                    for i in item.iter('datasource-dependencies')
                }})
                arr.append(res)
        self.result.update({'dashboards': arr})


def create_json(f_name):
    tb = TableauParser(f_name)

    twb_dict = tb.create_json()

    return json.dumps(twb_dict)
