#!/usr/bin/env python

import json
import subprocess
import sys


def tags_to_influx(tags):
    if tags:
        return ',' + ','.join(['{}={}'.format(k,v) for k,v in tags.items()])

    return ''


def json_to_influxdb(payload, path='', tags=None):
    separator = '.'
    path = path[1:] if path.startswith(separator) else path
    for key, item in payload.items():
        key = key.strip()
        if isinstance(item, dict):
            json_to_influxdb(item, path + separator + key, tags)
        elif isinstance(item, list) or isinstance(item, tuple):
            for _ in item:
                port = ''
                if isinstance(_, dict) and path == 'cluster' and key in ['processes', 'machines']:
                    ptags = {
                        'address': _.pop('address'),
                        'machine_id': _.pop('machine_id'),
                        'excluded': _.pop('excluded'),
                    }
                    if key == 'processes':
                        ptags['process_id'] = _.pop('process')
                        address, port = ptags['address'].split(':')
                        ptags['address'] = address
                        port = separator + port
                    json_to_influxdb(_, path + separator + key + port, ptags)
        else:
            try:
                output_value = float(item)

                if output_value != float('inf') and output_value != float('-inf'):
                    print path + tags_to_influx(tags) + ' ' + key + "={:.9f}".format(output_value)
            except:
                pass



def main(cluster_file):

    results = subprocess.Popen(
        [
            "fdbcli",
            "-C", cluster_file,
            "--exec", "status json"
        ],
        stdout=subprocess.PIPE
    )
    payload = results.stdout.read()

    try:
        fdb_status = json.loads(payload)
    except ValueError:
        return

    try:
        coordinator_health = fdb_status.get('client', {}).get('coordinators', {})
        quorum_reachable = coordinator_health.get('quorum_reachable', False)
        coordinators = coordinator_health.get('coordinators', [])
        reachable = 0
        total_coordinators = len(coordinators)
        for coordinator in coordinators:
            reachable += 1 if coordinator.get('reachable', False) else 0

        processes = fdb_status.get('cluster', {}).pop('processes', {})
        friendly_processes = []
        for proc in processes.keys():
            friendly_process = processes[proc]
            friendly_process['process'] = proc
            friendly_process['messages'] = len(friendly_process.get('messages', []))

            friendly_processes.append(friendly_process)

        fdb_status['cluster']['processes'] = friendly_processes

        machines = fdb_status.get('cluster', {}).pop('machines', {})
        friendly_machines = []
        for machine in machines.keys():
            friendly_machine = machines[machine]
            friendly_machines.append(friendly_machine)

        fdb_status['cluster']['machines'] = friendly_machines

        # replacement slug for coordinators under client
        telemetry_friendly = {
            'total': total_coordinators,
            'reachable': reachable,
            'quorum_reachable': 1 if quorum_reachable else 0,
        }

        fdb_status['client']['coordinators'] = telemetry_friendly
    except Exception as ex:
        print ex
        return

    json_to_influxdb(fdb_status)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        exit(1)

    main(sys.argv[1])
