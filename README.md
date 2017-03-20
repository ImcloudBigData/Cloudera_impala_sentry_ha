# README #

* Cloudera Manager의 API를 이용하여 Impala(Catalog Serve, StateStore), Sentry(Sentry Server)의 Role을 다른 서버로 옮겨 실행하는 스크립트 입니다.
* kerberos를 사용하고 있다면 Keytab 재 생성 로직이 포함되어 있습니다.
* [Imcloud](http://www.imcloud.co.kr/)에서 배포합니다.

### How do I get set up? ###
* pip install cm_api
* Clone git clone https://github.com/ImcloudBigData/Cloudera_impala_sentry_ha.git
* vi conf/config.cfg
* run
    * bin/impala_start.sh
    * bin/sentry_start.sh

### Config Details. ###
* conf/config.cfg
* Cloudera Manager Setting
    * host = Cloudera Manager Server IP
    * port = Cloudera Manager PORT
    * cm_id = Cloudera Manager Login ID
    * cm_password = Cloudera Manager Login Password
    * api_version = Cloudera Manager Support Api Version
    * cdh_version = CDH Version

* Cluster Setting
    * cluster_name = Target Cluster name
    * max_wait_secs_role = Role restart wait
    * max_wait_secs = Service restart wait

* Impala
    * impala_service_name = Service Name
    * impala_role_types = Role Type
    * impala_role_restore_servers = HA Target Server Host Name, must two

* Sentry
    * sentry_restore_target_servers = HA Target Server Host Name, must two
    * sentry_service_name = Service Name
    * sentry_auto_restart_target = Relevant services
    * sentry_delay_secs = Check kerberos setting and wait