import subprocess
import random
import string
import json
import argparse
import sys
import socket

DEFAULT_PORT = 8000

def rand_alphanum_lower(l):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=l))

def get_endpoints(endpoints):
    if endpoints:
        return endpoints

    hostname = socket.getfqdn()

    return 'http://%s:%d' % (hostname, DEFAULT_PORT)

class JSONObj:
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

class RGWZone(JSONObj):
    def __init__(self, zone_dict):
        self.id = zone_dict['id']
        self.name = zone_dict['name']
        self.endpoints = zone_dict['endpoints']

class RGWZoneGroup(JSONObj):
    def __init__(self, zg_dict):
        self.id = zg_dict['id']
        self.name = zg_dict['name']
        self.api_name = zg_dict['api_name']
        self.is_master = zg_dict['is_master']
        self.endpoints = zg_dict['endpoints']

        self.zones_by_id = {}
        self.zones_by_name = {}

        for zone in zg_dict['zones']:
            self.zones_by_id[zone['id']] = RGWZone(zone)
            self.zones_by_name[zone['name']] = RGWZone(zone)

class RGWPeriod(JSONObj):
    def __init__(self, period_dict):
        self.id = period_dict['id']
        self.epoch = period_dict['epoch']
        pm = period_dict['period_map']
        self.zonegroups_by_id = {}
        self.zonegroups_by_name = {}

        for zg in pm['zonegroups']:
            self.zonegroups_by_id[zg['id']] = RGWZoneGroup(zg)
            self.zonegroups_by_name[zg['name']] = RGWZoneGroup(zg)

class RGWAccessKey(JSONObj):
    def __init__(self, d):
        self.uid = d['user']
        self.access_key = d['access_key']
        self.secret_key = d['secret_key']

class RGWUser(JSONObj):
    def __init__(self, d):
        self.uid = d['user_id']
        self.display_name = d['display_name']
        self.email = d['email']

        self.keys = []

        for k in d['keys']:
            self.keys.append(RGWAccessKey(k))

        is_system = d.get('system') or 'false'
        self.system = (is_system == 'true')



class RGWAMException(BaseException):
    def __init__(self, message):
        self.message = message


class RGWAdminCmd:
    def __init__(self):
        self.cmd_prefix = [ 'radosgw-admin' ]

    def run(self, cmd):
        run_cmd = self.cmd_prefix + cmd
        result = subprocess.run(run_cmd, stdout=subprocess.PIPE)
        return (result.returncode, result.stdout)

class RGWCmd:
    def __init__(self):
        self.cmd_prefix = [ 'radosgw' ]

    def run(self, cmd):
        run_cmd = self.cmd_prefix + cmd
        result = subprocess.run(run_cmd, stdout=subprocess.PIPE)
        return (result.returncode, result.stdout)

class RealmOp(RGWAdminCmd):
    def __init__(self):
        RGWAdminCmd.__init__(self)
        
    def create(self, name = None, is_default = True):
        self.name = name
        if not self.name:
            self.name = 'realm-' + rand_alphanum_lower(8)

        params = [ 'realm',
                   'create',
                   '--rgw-realm', self.name ]

        if is_default:
            params += [ '--default' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class ZonegroupOp(RGWAdminCmd):
    def __init__(self):
        RGWAdminCmd.__init__(self)
        
    def create(self, realm, name = None, endpoints = None, is_master = True, is_default = True):
        self.name = name
        if not self.name:
            self.name = 'zg-' + rand_alphanum_lower(8)

        params = [ 'zonegroup',
                   'create',
                   '--rgw-realm', realm,
                   '--rgw-zonegroup', self.name,
                   '--endpoints', endpoints ]

        if is_master:
            params += [ '--master' ]

        if is_default:
            params += [ '--default' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class ZoneOp(RGWAdminCmd):
    def __init__(self):
        RGWAdminCmd.__init__(self)
        
    def create(self, realm, zonegroup, name = None, endpoints = None, is_master = True, is_default = True):
        self.name = name
        if not self.name:
            self.name = 'zg-' + rand_alphanum_lower(8)

        params = [ 'zone',
                   'create',
                   '--rgw-realm', realm,
                   '--rgw-zonegroup', zonegroup,
                   '--rgw-zone', self.name,
                   '--endpoints', endpoints ]

        if is_master:
            params += [ '--master' ]

        if is_default:
            params += [ '--default' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class PeriodOp(RGWAdminCmd):
    def __init__(self):
        RGWAdminCmd.__init__(self)
        
    def update(self, realm, commit = True):

        params = [ 'period',
                   'update',
                   '--rgw-realm', realm ]

        if commit:
            params += [ '--commit' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class UserOp(RGWAdminCmd):
    def __init__(self):
        RGWAdminCmd.__init__(self)
        
    def create(self, uid = None, uid_prefix = None, display_name = None, email = None, is_system = False):
        self.uid = uid
        if not self.uid:
            prefix = uid_prefix or 'user'
            self.uid = prefix + '-' + rand_alphanum_lower(6)

        self.display_name = display_name
        if not self.display_name:
            self.display_name = self.uid

        params = [ 'user',
                   'create',
                   '--uid', self.uid,
                   '--display-name', self.display_name ]

        if email:
            params += [ '--email', email ]

        if is_system:
            params += [ '--system' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class RGWAM:
    def __init__(self):
        pass

    def realm_bootstrap(self, realm, zonegroup, zone, endpoints, sys_uid, uid):
        endpoints = get_endpoints(endpoints)

        realm_info = RealmOp().create(realm)
        if not realm_info:
            return

        realm_name = realm_info['name']
        realm_id = realm_info['id']
        print('Created realm %s (%s)' % (realm_name, realm_id))

        zg_info = ZonegroupOp().create(realm_name, zonegroup, endpoints, True, True)

        zg_name = zg_info['name']
        zg_id = zg_info['id']
        print('Created zonegroup %s (%s)' % (zg_name, zg_id))

        zone_info = ZoneOp().create(realm_name, zg_name, zone, endpoints, True, True)

        zone_name = zone_info['name']
        zone_id = zone_info['id']
        print('Created zone %s (%s)' % (zone_name, zone_id))

        period_info = PeriodOp().update(realm_name, True)

        period = RGWPeriod(period_info)

        print('Period: ' + period.id)

        sys_user_info = UserOp().create(uid = sys_uid, uid_prefix = 'user-sys', is_system = True)

        sys_user = RGWUser(sys_user_info)

        print('Created system user: %s' % sys_user.uid)

        user_info = UserOp().create(uid = uid, is_system = False)

        user = RGWUser(user_info)

        print('Created regular user: %s' % user.uid)


    def run_radosgw(self, port = None, log_file = None, debug_ms = None, debug_rgw = None):

        fe_cfg = 'beast'
        if port:
            fe_cfg += ' port=%s' % port


        params = [ '--rgw-frontends', fe_cfg ]

        if log_file:
            params += [ '--log-file', log_file ]

        if debug_ms:
            params += [ '--debug-ms', debug_ms ]

        if debug_rgw:
            params += [ '--debug-rgw', debug_rgw ]

        RGWCmd().run(params)


class RealmCommand:
    def __init__(self, args):
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='S3 control tool',
            usage='''rgwam realm <subcommand>

The subcommands are:
   bootstrap                     Manipulate bucket versioning
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def bootstrap(self):
        parser = argparse.ArgumentParser(
            description='Bootstrap new realm',
            usage='rgwam realm bootstrap [<args>]')
        parser.add_argument('--realm')
        parser.add_argument('--zonegroup')
        parser.add_argument('--zone')
        parser.add_argument('--endpoints')
        parser.add_argument('--sys-uid')
        parser.add_argument('--uid')

        args = parser.parse_args(self.args[1:])

        RGWAM().realm_bootstrap(args.realm, args.zonegroup, args.zone, args.endpoints,
                                args.sys_uid, args.uid)


class ZoneCommand:
    def __init__(self, args):
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='S3 control tool',
            usage='''rgwam zone <subcommand>

The subcommands are:
   run                     run radosgw daemon in current zone
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def run(self):
        parser = argparse.ArgumentParser(
            description='Run radosgw daemon',
            usage='rgwam zone run [<args>]')
        parser.add_argument('--port')
        parser.add_argument('--log-file')
        parser.add_argument('--debug-ms')
        parser.add_argument('--debug-rgw')

        args = parser.parse_args(self.args[1:])

        RGWAM().run_radosgw(port = args.port)


class TopLevelCommand:

    def _parse(self):
        parser = argparse.ArgumentParser(
            description='RGW assist for multisite tool',
            usage='''rgwam <command> [<args>]

The commands are:
   realm bootstrap               Bootstrap realm
   zone run                      Run radosgw in current zone
''')
        parser.add_argument('command', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command) or args.command[0] == '_':
            print('Unrecognized command:', args.command)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.command)

    def realm(self):
        cmd = RealmCommand(sys.argv[2:]).parse()
        cmd()

    def zone(self):
        cmd = ZoneCommand(sys.argv[2:]).parse()
        cmd()


def main():
    cmd = TopLevelCommand()._parse()
    try:
        cmd()
    except RGWAMException as e:
        print('ERROR: ' + e.message)


if __name__ == '__main__':
    main()

