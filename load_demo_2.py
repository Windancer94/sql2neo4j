#!/usr/bin/env python
# -*- coding: utf-8 -*
from importer import *


def main():
    '''
    自动导入

    '''
    source='jdbc:mysql://127.0.0.1:3306/?user=...&password=...'
    node_tables = ['''test.merge_content''','''test.merge_term''','''test.merge_entity''']
    relationship_tables = ['''test.merge_content_watching_term''','''test.merge_term_watching_entity''','''test.merge_entity_watching_entity''']
    w = worker()
    w.auto_import(source,node_tables,relationship_tables)

if __name__=='''__main__''':
    main()
