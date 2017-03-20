#!/usr/bin/env python
#-*- coding: utf-8 -*-

# Copyright 2017, IMCLOUD Inc.
# All rights reserved.

import os
import logging
import logging.handlers
from comConfig import logger_conf as conf

# 공통 로거
class CommonLogger():

    def __init__(self):

        # 로거 인스턴스 생성
        self.logger = logging.getLogger('opensight_inside_logger')

        # 포매터 생성
        fomatter = logging.Formatter('[%(levelname)s| %(filename)s:%(lineno)s] %(asctime)s '
                                     '> %(message)s')

        # 환경변수를 읽어서 로깅 레벨과 로그를 남길 파일의 경로를 변수에 저장한다
        try:
            if (os.environ['NODE_ENV'] == 'local'):
                loggerLevel = logging.DEBUG
                filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../logs/',
                                        conf.get('filenm_debug'))
            elif(os.environ['NODE_ENV'] == 'test'):
                loggerLevel = logging.DEBUG
                filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../logs/',
                                        conf.get('filenm_test'))
            else:
                loggerLevel = logging.INFO
                filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../logs/',
                                        conf.get('filenm_info'))
        except:
            loggerLevel = getattr(logging, conf.get('default_level').upper())
            filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../logs/',
                                    conf.get('filenm_debug'))

        # 스트림과 파일로 로그를 출력하는 핸들러 생성
        # 로그 파일 용량 제한(100MB)
        fileMaxByte = conf.get('maxfile')
        fileHandler = logging.handlers.RotatingFileHandler(filename, encoding='utf-8',
                                                           maxBytes=fileMaxByte, backupCount=10)
        streamHandler = logging.StreamHandler()

        # 각 핸들러에 포매터 지정
        fileHandler.setFormatter(fomatter)
        streamHandler.setFormatter(fomatter)

        # 로거 인스턴스에 스트림 핸들러, 파일핸들러, 몽고디비핸들러 적용
        self.logger.addHandler(fileHandler)
        self.logger.addHandler(streamHandler)

        # 로거 레벨 설정
        self.logger.setLevel(loggerLevel)

clog = CommonLogger().logger

# 테스트
if __name__ == '__main__':
    clog = CommonLogger().logger
    clog.info('START.....')
    clog.debug('Write Start')
    clog.debug("test debug")
    clog.info("test info")
    clog.warning("test warning")
    clog.error("test error")
    clog.critical("test critical")
