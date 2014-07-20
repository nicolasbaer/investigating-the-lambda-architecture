#!/bin/bash

. ./global.sh


application=$1


# install the java virtual machine and sets the JAVA_HOME env variable
function install_jre(){
    JAVA_HOME=$job_home/jre

    if [ ! -d "$JAVA_HOME" ]; then
        mkdir -p $JAVA_HOME
        cp -r $lambda_home_install/jre/* $JAVA_HOME/
    fi

    PATH=$PATH:/$lambda_home_install/jre/bin
}

function install_app(){
    local app_name=$1
    local app_root=$job_home/$app_name
    local app_home=$app_root/home
    local app_data=$app_root/data
    local app_logs=$app_root/logs
    local app_pids=$app_root/pids
    local app_tmp=$app_root/tmp

    if [ ! -d "$app_home" ]; then
        mkdir -p $app_home
        cp -r $lambda_home_install/$app_name/* $app_home/

        mkdir -p $app_data
        mkdir -p $app_logs
        mkdir -p $app_pids
        mkdir -p $app_tmp
    fi

    LAMBDA_APP_HOME=$app_home
    LAMBDA_APP_DATA=$app_data
    LAMBDA_APP_LOGS=$app_logs
    LAMBDA_APP_PIDS=$app_pids
    LAMBDA_APP_TMP=$app_tmp
}


if [ $application = "jre" ]; then
    install_jre
else
    install_app $application
fi
