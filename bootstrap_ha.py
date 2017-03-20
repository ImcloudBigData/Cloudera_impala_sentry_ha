#!/usr/bin/env python
#-*- coding: utf-8 -*-

# Copyright 2017, IMCLOUD Inc.
# All rights reserved.
# 데몬 구성

import datetime
import time
import sys

from app.common.comLogger import clog
from app.cloudera import cloudera_cm as cm

"""
 데몬 구동 파라미터 처리
"""
def bootstrap(arg):
    print('Start setting....')
    print('arg',arg)
    if arg == 'impala':
        print('impala')
        cm_handler = cm.ClouderaHandler()
        cm_handler.impala_service_role_setting()
    elif arg == 'sentry':
        print('sentry')
        cm_handler = cm.ClouderaHandler()
        cm_handler.sentry_service_role_setting()
    else:
        print('Argument is not match!')


    # cm_handler = cm.ClouderaHandler()
    # cm_handler.fn_test()

    #from_time = datetime.datetime.fromtimestamp(time.time() - 1800)
    #to_time = datetime.datetime.fromtimestamp(time.time())

# 데몬 서버 구동
if __name__ == '__main__':
    if len(sys.argv) > 1:
        bootstrap(sys.argv[1])
    else:
        print('==============================================================')
        print('| How to use                                                 |')
        print('| python bootstrap_ha impala | is imapla setting start       |')
        print('| python bootstrap_ha sentry | is sentry setting start       |')
        print('|                                                            |')
        print('| Config file path -> conf/config.cfg                        |')
        print('|                                                            |')
        print('| Caution sentry                                             |')
        print('| jdbc must be installed on the target server.               |')
        print('| /opt/cloudera/parcels/CDH-<version>/lib/sentry/lib         |')
        print('| OR                                                         |')
        print('| /usr/share/                                                |')
        print('| hive,hue,impala is restart                                 |')
        print('==============================================================')
