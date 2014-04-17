#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from flask import Flask, request, render_template, redirect, url_for
from werkzeug.contrib.cache import SimpleCache

# application setup
app = Flask(__name__)
cache = SimpleCache()


@app.route('/')
def list_services():
    s = cache.get("services")
    services = dict()
    if s:
        services = json.loads(s)

    services_list = list()
    for k, v in services.iteritems():
        s = lambda: None
        s.host = v["host"]
        s.node_num = v["node_num"]
        s.service = v["service"]
        s.status = v["status"]
        s.uuid = k
        services_list.append(s)

    print services

    return render_template("index.html", services=services_list)


@app.route('/node/<uuid>.<status>')
def node(uuid, status):
    s = cache.get("services")
    services = dict()
    if s:
        services = json.loads(s)

    service = services[uuid]
    service["status"] = status
    services[uuid] = service

    cache.set("services", json.dumps(services))

    return redirect(url_for('list_services'))


@app.route('/status/<uuid>')
def status(uuid):
    s = cache.get("services")
    services = json.loads(s)
    return services[uuid]['status']


@app.route('/register/<node_num>.<host>.<service>.<uuid>')
def register(node_num, host, service, uuid):
    """

    :param node_num:
    :param host:
    :param service:
    :param uuid:
    :return:
    """
    s = cache.get("services")
    services = dict()
    if s:
        services = json.loads(s)

    print s

    services[uuid] = {"node_num": node_num, "host": host, "service": service, "status": "running"}

    cache.set("services", json.dumps(services))

    return "ok"


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
