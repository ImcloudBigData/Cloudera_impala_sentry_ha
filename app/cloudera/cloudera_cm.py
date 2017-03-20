#!/usr/bin/env python
#-*- coding: utf-8 -*-

# Copyright 2017, IMCLOUD Inc.
# All rights reserved.


import time
import hashlib
import datetime
# import json
from cm_api.api_client import ApiResource

from cm_api.endpoints import cms
from cm_api.endpoints import roles
from cm_api.endpoints import hosts
from cm_api.endpoints import clusters
from cm_api.endpoints import services
from cm_api.endpoints.services import ApiService

from app.common.comLogger import clog
from app.common.comConfig import cm_conf as conf

class ClouderaHandler():
    # clog.info('[ START-ClouderaHandler class ]..........')

    def __init__(self):
        # clog.info('[ START-ClouderaHandler init ]..........')

        self.resource = ApiResource(conf.get('host'), conf.get('port'), conf.get('cm_id'),
                                    conf.get('cm_password'), version=conf.get('api_version'))

    # Cloudera Manager connection test
    def fn_get_all_cluster(self):
        clusters = self.resource.get_all_clusters()
        return clusters

    def fn_get_cluster(self, cluster_nm):
        cluster = None
        if cluster_nm is None:
            for c in self.resource.get_all_clusters():
                if c.version == conf.get('cdh_verion'):
                    cluster = c
        else:
            cluster = self.resource.get_cluster(cluster_nm)

        return cluster

    def find_cluster(self, cluster_name):
        if cluster_name:
            cluster = self.resource.get_cluster(cluster_name)
        else:
            all_clusters = self.resource.get_all_clusters()
            if len(all_clusters) == 0:
                raise Exception("No clusters found; create one before calling this script")
            if len(all_clusters) > 1:
                raise Exception("Cannot use implicit cluster; there is more than one available")
            cluster = all_clusters[0]
        clog.info("Found cluster: %s" % (cluster.displayName))
        return cluster

    # CM에 보이는 displayname을 CM내부에서 다루는 이름으로 변환
    def display_to_name(self, impala_display_name):
        all_clusters = self.resource.get_all_clusters()
        for cluster_name in range(len(all_clusters)):
            # clog.info(all_clusters[cluster_name].displayName)
            for service in all_clusters[cluster_name].get_all_services():
                if impala_display_name == service.displayName:
                    # clog.info('displayName',service.displayName)  # displa name
                    # clog.info('name',service.name)  # CM inside setting name
                    # clog.info('config',service.get_config())
                    return service.name
        raise Exception("can't find service name. check the 'config.cfg file : impala_service_name=XXXXXX'")

    # rule name은 CM내부적으료 사용되는 unique한 이름값임
    def find_the_rule_name(self, service_name, cluster_name, rule_type):
        role_value_list = roles.get_all_roles(self.resource, service_name, cluster_name=cluster_name, view=None)
        # clog.info(role_value_list)
        for role_value in range(len(role_value_list)):
            # clog.info(role_value_list[role_value].name)  # CM service-role unique id
            if rule_type in role_value_list[role_value].name:
                # clog.info('rule_name', role_value_list[role_value].name)
                return role_value_list[role_value].name

        return "not_find_ruletype"  # 해당 룰이 존재하지 않는 경우
        # raise Exception("can't find service name. rule name. service_name:", service_name)

    # # rule name은 CM내부적으료 사용되는 unique한 이름값임 모두를 찾아 리턴함
    # def find_the_service_name(self, service_name, cluster_name):
    #     role_value_list = roles.get_all_roles(self.resource, service_name, cluster_name=cluster_name, view=None)
    #
    #     service_names = []
    #     for role_value in range(len(role_value_list)):
    #         service_names.append(role_value_list[role_value].name)
    #
    #     return service_names

    # 서비스 stop
    def service_role_stop(self, service_name, rule_name):
        service_get = services.get_service(self.resource, service_name, cluster_name=conf.get('cluster_name'))
        service_stop_result = service_get.stop_roles(rule_name)  # role stop
        self.check_service_role_state(service_get, rule_name, 'STOPPED')    # 정지되었는지를 확인
        clog.info('service_stop_finish!!')

    # 서비스 start
    def service_role_start(self, service_name, rule_name):
        service_get = services.get_service(self.resource, service_name, cluster_name=conf.get('cluster_name'))
        service_stop_result = service_get.start_roles(rule_name)  # role stop
        self.check_service_role_state(service_get, rule_name, 'STARTED')    # 시작되었는지를 확인
        clog.info('service_start_finish!!')

    # 서비스 role 상태 변경을 대기하는 function
    def check_service_role_state(self, service_get, rule_name, check_state):
        MAX__WAIT_SECS_ROLE = int(conf.get('max_wait_secs_role'))    # 최대 30분을 대기함
        # MAX_WAIT_SECS = 60   # 테스트를 위한 대기시간
        clog.info('check_service_role_state')
        clog.info('get_role(rule_name.roleState) %s' % (service_get.get_role(rule_name).roleState))  # 하나의 롤에 대한 상태를 가져옴. 롤링하면서 상태가 완료될때까지 대기하는 기준 변수

        for attempt in xrange(1, MAX__WAIT_SECS_ROLE + 1):     # 최대 30분을 대기함
            new_role = service_get.get_role(rule_name).roleState  # role의 상태를 가져옴
            if new_role == check_state:
                # 상태가 변경되면 return을 함
                clog.info('check second %s to %s wait time.sleep(1) check state %s to %s'% (attempt,str(MAX__WAIT_SECS_ROLE),new_role,check_state) )
                return
            else:
                # 상태가 변경되지 않으면 1초를 대기
                clog.info('check second %s to %s wait time.sleep(1) check state %s to %s' % (attempt, str(MAX__WAIT_SECS_ROLE), new_role, check_state))
            time.sleep(1)
        else:
            # 대기시간 동안 상태가 변경되지 않으면 에러를 던짐
            raise Exception('check service role state error. No state change for a long time. max 30min')

    # 서비스 상태 변경을 대기하는 function
    def check_service_state(self, service_name, check_state):
        clog.info('check_service_state')
        MAX_WAIT_SECS = int(conf.get('max_wait_secs_role'))  # 최대 30분을 대기함
        # MAX_WAIT_SECS_ROLE = 60   # 테스트를 위한 대기시간
        for attempt in xrange(1, MAX_WAIT_SECS + 1):  # 최대 30분을 대기함
            new_serviceState = services.get_service(self.resource, service_name, cluster_name=conf.get('cluster_name')).serviceState  # role의 상태를 가져옴
            if new_serviceState == check_state:
                # 상태가 변경되면 return을 함
                clog.info('check second %s to %s wait time.sleep(1) check state %s to %s' % ( attempt, str(MAX_WAIT_SECS), new_serviceState, check_state))
                return
            else:
                # 상태가 변경되지 않으면 1초를 대기
                clog.info('check second %s to %s wait time.sleep(1) check state %s to %s' % ( attempt, str(MAX_WAIT_SECS), new_serviceState, check_state))
            time.sleep(1)
        else:
            # 대기시간 동안 상태가 변경되지 않으면 에러를 던짐
            raise Exception('check service role state error. No state change for a long time. max 30min')

    # 서비스 role 삭제
    def service_role_delete(self, service_name, rule_name):
        clog.info('service_delete')
        service_get = services.get_service(self.resource, service_name, cluster_name=conf.get('cluster_name'))
        service_delete_result = service_get.delete_role(rule_name)  # role stop
        clog.info('delay 5sec start')
        time.sleep(5)   # 임의로 5초 대기
        clog.info('delay 5sec finish')
        clog.info('service_delete_finish!!')

    # 새로운 service의 role을 create함
    def service_role_create(self, service_name, rule_type, restore_target_server):
        clog.info('service_create')

        host_id = ''
        get_all_hosts = hosts.get_all_hosts(self.resource, view=None)   #모든 host 정보를 찾음
        for rcg in get_all_hosts:

            # host 정보에서 role를 설치할 서버의 hostId를 찾음
            if rcg.hostname == restore_target_server: # conf.get('restore_target_server'):
                host_id = rcg.hostId
                clog.info('rcg.hostId %s'% (rcg.hostId))
                clog.info('rcg.hostname %s'% (rcg.hostname))

        service_get = services.get_service(self.resource, service_name, cluster_name=conf.get('cluster_name'))
        if host_id != '':
            md5 = hashlib.md5()
            md5.update(host_id)
            new_role_name = "%s-%s-%s" % (service_name, rule_type, md5.hexdigest())
            clog.info('new_role_name %s' % (new_role_name))
            # create_role(self, role_name, 'CATALOGSERVER', 'hadoop08')
            service_create_result = service_get.create_role(new_role_name, rule_type, restore_target_server) #
        else:
            raise Exception('not match set restore_target_server')

        return service_create_result

    # service_name과 rule_name을 조회하는 function
    def get_service_name_rule_name(self, find_service_name, role_type):
        clog.info('get_service_name_rule_name')
        # Impala Catalog Server
        # displayName을 serviceName으로 변경
        service_name = self.display_to_name(find_service_name) # conf.get('impala_service_name')
        clog.info('service_name %s' % (service_name))

        # find the role_names
        rule_name = self.find_the_rule_name(service_name, conf.get('cluster_name'), role_type)
        clog.info('rule_name %s' % (rule_name))
        clog.info('service_name %s' % (service_name))
        return service_name, rule_name

    # 서비스 재 시작
    def service_restart(self, service_name):
        clog.info('service_restart')
        service_get = services.get_service(self.resource, service_name, cluster_name=conf.get('cluster_name'))
        service_get.restart()

        # 재 시작 완료 대기
        self.check_service_state(service_name, 'STARTED')

    # 변경된 설정 deploy
    def service_deploy_client_config(self, service_name, rule_name):
        clog.info('service_deploy_client_config')
        # service_get = services.get_service(self.resource, service_name, cluster_name=conf.get('cluster_name'))
        # service_get.deploy_client_config(rule_name)
        clusters_get = clusters.get_cluster(self.resource, conf.get('cluster_name'))
        clusters_get.deploy_client_config()
        clog.info('delay 5 sec start')
        time.sleep(5)
        clog.info('delay 5 sec finish')

    # 재 실행 해야 하는 상태를 체크하고 restart
    def get_all_staleness_state(self):
        clog.info('get_all_staleness_state')
        auto_restart_flag = False
        all_clusters = self.resource.get_all_clusters()
        for cluster_number in range(len(all_clusters)):
            for each_service in all_clusters[cluster_number].get_all_services():
                if 'STALE' == each_service.configStalenessStatus:
                    clog.info('each_service configStalenessStatus %s %s' % (each_service.displayName, each_service.configStalenessStatus))
                    service_name =  self.display_to_name(each_service.displayName) # displayname to service
                    self.service_restart(service_name)
                    auto_restart_flag = True

        # 자동으로 restart해야할 service가 파악되지 않은 경우 'sentry_auto_restart_target' 조건으로 처리
        if auto_restart_flag == False:
            restart_traget_service_names = conf.get('sentry_auto_restart_target').split(',')
            for target_service_name in restart_traget_service_names:
                target_service_name = target_service_name.strip()
                clog.info('target_service_name %s' % (target_service_name))
                service_name = self.display_to_name(target_service_name)  # displayname to service
                self.service_restart(service_name)

            clog.info('auto_restart_flag')
        # clog.info('each_service',each_services)
        # for each_service in each_services:
        #     each_service.restart()

    # 누락된 자격증명 생성을 통해서 새로 옮긴 role에 대한 keytab을 재 생성함
    def keytab_generate_credentials(self):
        ClouderaManager_result = cms.ClouderaManager(self.resource)
        generate_credential_result = ClouderaManager_result.generate_credentials()
        clog.info('keytab generate_credentials finish delay start')
        delay_secs = int(conf.get('sentry_delay_secs'))
        for delay_sec in range(delay_secs):
            time.sleep(1)
            clog.info('delay %s sec to %s' % (delay_sec, delay_secs))
        clog.info('keytab generate_credentials finish delay finish')


    # default
    # service role 서버를 옮겨서 다시 실행하를 로직, role type만 적으면 내부적으로 동작함
    def default_service_role_setting(self, find_service_name, role_type):
        active_host_name = self.find_activate_service_role(find_service_name, role_type)  # 현재 동작 중인 Active hostname을 찾음

        # Cloudera에 접속 되는지 테스트.
        clog.info('1. step service alive check ========================')
        sentry_role_restore_servers_ha_hashmap = conf.get('sentry_restore_target_servers')
        target_host_name = sentry_role_restore_servers_ha_hashmap[active_host_name]
        clog.info('target host server %s' % target_host_name)
        restore_target_server = target_host_name

        # Cloudera에 접속 되는지 테스트.
        clog.info('1. step service alive check ========================')
        clog.info('%s' % (self.find_cluster(conf.get('cluster_name'))))

        # service_name과 rule_name을 조회
        clog.info('2. service role find check =========================')
        service_name, rule_name = self.get_service_name_rule_name(find_service_name, role_type)

        # 서비스가 존재하지 않으면 정지 + 삭제 하는 로직을 타지 않음
        if rule_name != 'not_find_ruletype':
            # 기존 Service 중지 + 중지 완료를 대기
            clog.info('2-1. service role stop =========================')
            service_role_stop_result = self.service_role_stop(service_name, rule_name)
            clog.info('service_role_stop_result %s'  % (service_role_stop_result))

            clog.info('2-2. service role delete =======================')
            # 기존 서비스의 role 삭제. (stop안된 상태여도 삭제는 되나 안전하게 stop한 후 service role를 삭제)
            service_role_delete_result = self.service_role_delete(service_name, rule_name)
            clog.info('service_role_delete_result %s' % (service_role_delete_result))

        # service role을 create
        clog.info('3.1. service role create =============================')
        service_role_create_result = self.service_role_create(service_name, role_type, restore_target_server)
        clog.info('service_role_create_result %s' % (service_role_create_result))
        clog.info('create delay 2 sec start')
        time.sleep(2)
        clog.info('create delay 2 sec stop')

        # 누락된 자격증명 재 성성을 통해 Keytab을 재 생성
        clog.info('3.2. service role create =============================')
        self.keytab_generate_credentials()

        # service_name과 rule_name을 조회
        clog.info('4. service role start ==============================')
        service_name, rule_name = self.get_service_name_rule_name(find_service_name, role_type)

        # Service 시작 + 완료를 대기s
        service_start_result = self.service_role_start(service_name, rule_name)
        clog.info('service_start_result %s' % (service_start_result))

        if role_type != 'SENTRY_SERVER':
            # 서비스 role 설정후 서비스 재 실행
            clog.info('5. service restart =================================')
            service_restart_result = self.service_restart(service_name)
        else:
            clog.info('5-1. service deploy client config ============================')
            service_deploy_client_config_result = self.service_deploy_client_config(service_name, rule_name)

            clog.info('5-2. service deploy client restart ============================')
            self.get_all_staleness_state()


        clog.info('6. finish ==========================================')

    # 서비스의 롤 두개를 한번에 처리
    # 서비스를 구분지어서 묶음 처리
    def default_service_role_setting2(self, find_service_name, role_types, restore_target_server):
        clog.info('default_service_role_setting2')
        # Cloudera에 접속 되는지 테스트.
        clog.info('1. step service alive check ========================')
        clog.info('%s' % (self.find_cluster(conf.get('cluster_name'))))

        for role_type in role_types.split(','):
            # service_name과 rule_name을 조회
            clog.info('2. service role find check =========================')
            service_name, rule_name = self.get_service_name_rule_name(find_service_name, role_type)

            # 서비스가 존재하지 않으면 정지 + 삭제 하는 로직을 타지 않음
            if rule_name != 'not_find_ruletype':
                # 기존 Service 중지 + 중지 완료를 대기
                clog.info('2-1. service role stop =========================')
                service_role_stop_result = self.service_role_stop(service_name, rule_name)
                clog.info('service_role_stop_result %s' % (service_role_stop_result))

                clog.info('2-2. service role delete =======================')
                # 기존 서비스의 role 삭제. (stop안된 상태여도 삭제는 되나 안전하게 stop한 후 service role를 삭제)
                service_role_delete_result = self.service_role_delete(service_name, rule_name)
                clog.info('service_role_delete_result %s' % (service_role_delete_result))

        for role_type in role_types.split(','):
            service_name, rule_name = self.get_service_name_rule_name(find_service_name, role_type)
            # service role을 create
            clog.info('3. service role create =============================')
            service_role_create_result = self.service_role_create(service_name, role_type, restore_target_server)
            clog.info('service_role_create_result %s' % (service_role_create_result))

        for role_type in role_types.split(','):
            # service_name과 rule_name을 조회
            clog.info('4. service role start ==============================')
            service_name, rule_name = self.get_service_name_rule_name(find_service_name, role_type)

            # # Service 시작 + 완료를 대기
            # service_start_result = self.service_role_start(service_name, rule_name)
            # clog.info('service_start_result', service_start_result)

        # 서비스 role 설정후 서비스 재 실행
        clog.info('5. service restart =================================')
        service_restart_result = self.service_restart(service_name)

        clog.info('6. finish ==========================================')

    # 현재 Acrive되어 있는 서버의 host명을 확인
    def find_activate_service_role(self, find_service_name, impala_role_types):
        clog.info('find_activate_service_role')

        hostname_str = ''
        for impala_role_type in impala_role_types.split(','):
            service_name, rule_name = self.get_service_name_rule_name(find_service_name, impala_role_type)
            service_get = services.get_service(self.resource, service_name, cluster_name=conf.get('cluster_name'))
            new_role_hostRef_hostId = service_get.get_role(rule_name).hostRef.hostId  # role의 상태를 가져옴
            hostname_str = hosts.get_host(self.resource, new_role_hostRef_hostId).hostname
            clog.info('check activate service role hostname %s' % (hostname_str))

        return hostname_str

    # def default_service_role_setting3(self, find_service_name, role_types, restore_target_server):
    def default_service_role_setting3(self, find_service_name, impala_role_types):
        clog.info('default_service_role_setting3')
        active_host_name = self.find_activate_service_role(find_service_name, impala_role_types)    # 현재 동작 중인 Active hostname을 찾음

        # Cloudera에 접속 되는지 테스트.
        clog.info('1. step service alive check ========================')
        clog.info('%s' % (self.find_cluster(conf.get('cluster_name'))) )
        clog.info('%s' % conf.get('impala_role_restore_servers'))
        impala_role_restore_servers_ha_hashmap = conf.get('impala_role_restore_servers')
        target_host_name = impala_role_restore_servers_ha_hashmap[active_host_name]
        clog.info('target host server %s' % target_host_name)
        restore_target_server = target_host_name
        for role_type in impala_role_types.split(','): # items() is python3.x
            clog.info('role_type %s' % (role_type))
            clog.info('restore_target_server %s ' % (restore_target_server))
        # for role_type in role_types.split(','):
            # service_name과 rule_name을 조회
            clog.info('2. service role find check =========================')
            service_name, rule_name = self.get_service_name_rule_name(find_service_name, role_type)

            # 서비스가 존재하지 않으면 정지 + 삭제 하는 로직을 타지 않음
            if rule_name != 'not_find_ruletype':
                # 기존 Service 중지 + 중지 완료를 대기
                clog.info('2-1. service role stop =========================')
                service_role_stop_result = self.service_role_stop(service_name, rule_name)
                clog.info('service_role_stop_result %s ' % (service_role_stop_result))

                clog.info('2-2. service role delete =======================')
                # 기존 서비스의 role 삭제. (stop안된 상태여도 삭제는 되나 안전하게 stop한 후 service role를 삭제)
                service_role_delete_result = self.service_role_delete(service_name, rule_name)
                clog.info('service_role_delete_result %s' % (service_role_delete_result))

        for role_type in impala_role_types.split(','):  # items() is python3.x
            service_name, rule_name = self.get_service_name_rule_name(find_service_name, role_type)
            # service role을 create
            clog.info('3. service role create =============================')
            service_role_create_result = self.service_role_create(service_name, role_type, restore_target_server)
            clog.info('service_role_create_result %s' % (service_role_create_result))

        for role_type in impala_role_types.split(','):  # items() is python3.x
            # service_name과 rule_name을 조회s
            clog.info('4. service role start ==============================')
            service_name, rule_name = self.get_service_name_rule_name(find_service_name, role_type)

        # 서비스 role 설정후 서비스 재 실행
        clog.info('5. service restart =================================')
        service_restart_resuslt = self.service_restart(service_name)

        clog.info('6. finish ==========================================')

    # impala 시작 main
    def impala_service_role_setting(self):
        clog.info('impala_service_role_setting')
        self.default_service_role_setting3(conf.get('impala_service_name'), conf.get('impala_role_types'))

    # sentry 시작 main
    def sentry_service_role_setting(self):
        print('sentry_service_role_setting')
        self.default_service_role_setting(conf.get('sentry_service_name'), 'SENTRY_SERVER')
    # test function
    def fn_test(self):
        clog.info('fn_test')
        # impala 관련하여 두개 재 할당과 실행
        # clog.info('conf.get(impala_role_types) %s' % conf.get('impala_role_types'))
        # self.default_service_role_setting3(conf.get('impala_service_name'), conf.get('impala_role_types'))

        ## 한개씩 서비스 추가 삭제 할때 사용. 두개를 동시에하면 restart가 중복되어 몇번 실행됨
        ## self.default_service_role_setting(conf.get('impala_service_name'), 'STATESTORE',conf.get('restore_target_server'))
        ## self.default_service_role_setting(conf.get('impala_service_name'), 'CATALOGSERVER', conf.get('restore_target_server'))
        ## 두개를 동시에 재 실행 하는 로직. target서버가 하나만 사용됨
        ## self.default_service_role_setting2(conf.get('impala_service_name'), 'STATESTORE,CATALOGSERVER', conf.get('impala_restore_target_server_catalog'))

        # sentry 재 할당
        self.default_service_role_setting(conf.get('sentry_service_name'), 'SENTRY_SERVER')

if __name__ == '__main__':
    cm_handler = ClouderaHandler()
    # mysql_handler.fn_conn_test()s
    cm_handler.fn_test()
