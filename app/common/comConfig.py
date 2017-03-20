#!/usr/bin/env python
#-*- coding: utf-8 -*-

# Copyright 2017, IMCLOUD Inc.
# All rights reserved.


import os
import json
import ConfigParser

# 인스턴스 생성
config = ConfigParser.RawConfigParser()

# 설정파일 읽기
config.read(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         '../../conf/config.cfg')))

# 설정 정보 변수(JSON)
init_conf = {}
cm_conf = {}
es_conf = {}
mysql_conf = {}
phoenix_conf = {}
kafka_conf = {}
logger_conf = {}
jdbc_conf = {}

# 기본 설정 정보 읽기
if config.has_section('init') == True :
    init_conf['process'] = config.get('init', 'process')


# Cloudera CM 설정 정보 읽기
if config.has_section('cloudera') == True :
    cm_conf['host'] = config.get('cloudera', 'host')
    cm_conf['port'] = config.getint('cloudera', 'port')
    cm_conf['cm_id'] = config.get('cloudera', 'cm_id')
    cm_conf['cm_password'] = config.get('cloudera', 'cm_password')
    cm_conf['api_version'] = config.getint('cloudera', 'api_version')
    cm_conf['cdh_verion'] = config.get('cloudera', 'cdh_verion')
    cm_conf['cluster_name'] = config.get('cloudera', 'cluster_name')

    cm_conf['impala_service_name'] = config.get('cloudera', 'impala_service_name')
    cm_conf['sentry_service_name'] = config.get('cloudera', 'sentry_service_name')
    # cm_conf['restore_target_server'] = config.get('cloudera', 'restore_target_server')

    cm_conf['sentry_auto_restart_target'] = config.get('cloudera', 'sentry_auto_restart_target')

    sentry_restore_target_servers = config.get('cloudera', 'sentry_restore_target_servers').split(',')
    sentry_restore_servers_ha = {}
    sentry_restore_servers_ha[sentry_restore_target_servers[0]] = sentry_restore_target_servers[1]
    sentry_restore_servers_ha[sentry_restore_target_servers[1]] = sentry_restore_target_servers[0]
    cm_conf['sentry_restore_target_servers'] = sentry_restore_servers_ha

    impala_role_restore_servers = config.get('cloudera', 'impala_role_restore_servers').split(',')
    restore_servers_ha = {}
    restore_servers_ha[impala_role_restore_servers[0]] = impala_role_restore_servers[1]
    restore_servers_ha[impala_role_restore_servers[1]] = impala_role_restore_servers[0]
    cm_conf['impala_role_restore_servers'] = restore_servers_ha
    cm_conf['impala_role_types'] = config.get('cloudera', 'impala_role_types')

    cm_conf['sentry_delay_secs'] = config.get('cloudera', 'sentry_delay_secs')

    cm_conf['max_wait_secs'] = config.get('cloudera', 'max_wait_secs')
    cm_conf['max_wait_secs_role'] = config.get('cloudera', 'max_wait_secs_role')


# 로거 설정 정보 읽기
if config.has_section('logger'):
    logger_conf['maxfile'] = config.getint('logger', 'maxfile')
    logger_conf['filenm_debug'] = config.get('logger', 'filenm.debug')
    logger_conf['filenm_test'] = config.get('logger', 'filenm.test')
    logger_conf['filenm_info'] = config.get('logger', 'filenm.info')
    logger_conf['default_level'] = config.get('logger', 'default.level')
    # logger_conf['db_save'] = config.getboolean('logger', 'db.save')

# 설정 정보 출력
def fn_conf_print():
    print(config.sections())

    print(init_conf)
    print(cm_conf)
    print(es_conf)
    print(mysql_conf)
    print(phoenix_conf)
    print(kafka_conf)
    print(logger_conf)
    print(jdbc_conf)

    print(es_conf.keys())
    print(es_conf.get('host'))

# 테스트
if __name__ == '__main__':
    fn_conf_print()