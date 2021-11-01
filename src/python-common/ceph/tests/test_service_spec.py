# flake8: noqa
import json
import yaml

import pytest

from ceph.deployment.service_spec import HostPlacementSpec, PlacementSpec, \
    ServiceSpec, RGWSpec, NFSServiceSpec, IscsiServiceSpec
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.hostspec import SpecValidationError


@pytest.mark.parametrize("test_input,expected, require_network",
                         [("myhost", ('myhost', '', ''), False),
                          ("myhost=sname", ('myhost', '', 'sname'), False),
                          ("myhost:10.1.1.10", ('myhost', '10.1.1.10', ''), True),
                          ("myhost:10.1.1.10=sname", ('myhost', '10.1.1.10', 'sname'), True),
                          ("myhost:10.1.1.0/32", ('myhost', '10.1.1.0/32', ''), True),
                          ("myhost:10.1.1.0/32=sname", ('myhost', '10.1.1.0/32', 'sname'), True),
                          ("myhost:[v1:10.1.1.10:6789]", ('myhost', '[v1:10.1.1.10:6789]', ''), True),
                          ("myhost:[v1:10.1.1.10:6789]=sname", ('myhost', '[v1:10.1.1.10:6789]', 'sname'), True),
                          ("myhost:[v1:10.1.1.10:6789,v2:10.1.1.11:3000]", ('myhost', '[v1:10.1.1.10:6789,v2:10.1.1.11:3000]', ''), True),
                          ("myhost:[v1:10.1.1.10:6789,v2:10.1.1.11:3000]=sname", ('myhost', '[v1:10.1.1.10:6789,v2:10.1.1.11:3000]', 'sname'), True),
                          ])
def test_parse_host_placement_specs(test_input, expected, require_network):
    ret = HostPlacementSpec.parse(test_input, require_network=require_network)
    assert ret == expected
    assert str(ret) == test_input

    ps = PlacementSpec.from_string(test_input)
    assert ps.pretty_str() == test_input
    assert ps == PlacementSpec.from_string(ps.pretty_str())

    # Testing the old verbose way of generating json. Don't remove:
    assert ret == HostPlacementSpec.from_json({
            'hostname': ret.hostname,
            'network': ret.network,
            'name': ret.name
        })

    assert ret == HostPlacementSpec.from_json(ret.to_json())




@pytest.mark.parametrize(
    "test_input,expected",
    [
        ('', "PlacementSpec()"),
        ("count:2", "PlacementSpec(count=2)"),
        ("3", "PlacementSpec(count=3)"),
        ("host1 host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1;host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1,host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1 host2=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='b')])"),
        ("host1=a host2=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name='a'), HostPlacementSpec(hostname='host2', network='', name='b')])"),
        ("host1:1.2.3.4=a host2:1.2.3.5=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='1.2.3.4', name='a'), HostPlacementSpec(hostname='host2', network='1.2.3.5', name='b')])"),
        ("myhost:[v1:10.1.1.10:6789]", "PlacementSpec(hosts=[HostPlacementSpec(hostname='myhost', network='[v1:10.1.1.10:6789]', name='')])"),
        ('2 host1 host2', "PlacementSpec(count=2, hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ('label:foo', "PlacementSpec(label='foo')"),
        ('3 label:foo', "PlacementSpec(count=3, label='foo')"),
        ('*', "PlacementSpec(host_pattern='*')"),
        ('3 data[1-3]', "PlacementSpec(count=3, host_pattern='data[1-3]')"),
        ('3 data?', "PlacementSpec(count=3, host_pattern='data?')"),
        ('3 data*', "PlacementSpec(count=3, host_pattern='data*')"),
        ("count-per-host:4 label:foo", "PlacementSpec(count_per_host=4, label='foo')"),
    ])
def test_parse_placement_specs(test_input, expected):
    ret = PlacementSpec.from_string(test_input)
    assert str(ret) == expected
    assert PlacementSpec.from_string(ret.pretty_str()) == ret, f'"{ret.pretty_str()}" != "{test_input}"'

@pytest.mark.parametrize(
    "test_input",
    [
        ("host=a host*"),
        ("host=a label:wrong"),
        ("host? host*"),
        ('host=a count-per-host:0'),
        ('host=a count-per-host:-10'),
        ('count:2 count-per-host:1'),
        ('host1=a host2=b count-per-host:2'),
        ('host1:10/8 count-per-host:2'),
        ('count-per-host:2'),
    ]
)
def test_parse_placement_specs_raises(test_input):
    with pytest.raises(SpecValidationError):
        PlacementSpec.from_string(test_input)

@pytest.mark.parametrize("test_input",
                         # wrong subnet
                         [("myhost:1.1.1.1/24"),
                          # wrong ip format
                          ("myhost:1"),
                          ])
def test_parse_host_placement_specs_raises_wrong_format(test_input):
    with pytest.raises(ValueError):
        HostPlacementSpec.parse(test_input)


@pytest.mark.parametrize(
    "p,hosts,size",
    [
        (
            PlacementSpec(count=3),
            ['host1', 'host2', 'host3', 'host4', 'host5'],
            3
        ),
        (
            PlacementSpec(host_pattern='*'),
            ['host1', 'host2', 'host3', 'host4', 'host5'],
            5
        ),
        (
            PlacementSpec(count_per_host=2, host_pattern='*'),
            ['host1', 'host2', 'host3', 'host4', 'host5'],
            10
        ),
        (
            PlacementSpec(host_pattern='foo*'),
            ['foo1', 'foo2', 'bar1', 'bar2'],
            2
        ),
        (
            PlacementSpec(count_per_host=2, host_pattern='foo*'),
            ['foo1', 'foo2', 'bar1', 'bar2'],
            4
        ),
    ])
def test_placement_target_size(p, hosts, size):
    assert p.get_target_count(
        [HostPlacementSpec(n, '', '') for n in hosts]
    ) == size


def _get_dict_spec(s_type, s_id):
    dict_spec = {
        "service_id": s_id,
        "service_type": s_type,
        "placement":
            dict(hosts=["host1:1.1.1.1"])
    }
    if s_type == 'nfs':
        pass
    elif s_type == 'iscsi':
        dict_spec['pool'] = 'pool'
        dict_spec['api_user'] = 'api_user'
        dict_spec['api_password'] = 'api_password'
    elif s_type == 'osd':
        dict_spec['spec'] = {
            'data_devices': {
                'all': True
            }
        }
    elif s_type == 'rgw':
        dict_spec['rgw_realm'] = 'realm'
        dict_spec['rgw_zone'] = 'zone'

    return dict_spec


@pytest.mark.parametrize(
    "s_type,o_spec,s_id",
    [
        ("mgr", ServiceSpec, 'test'),
        ("mon", ServiceSpec, 'test'),
        ("mds", ServiceSpec, 'test'),
        ("rgw", RGWSpec, 'realm.zone'),
        ("nfs", NFSServiceSpec, 'test'),
        ("iscsi", IscsiServiceSpec, 'test'),
        ("osd", DriveGroupSpec, 'test'),
    ])
def test_servicespec_map_test(s_type, o_spec, s_id):
    spec = ServiceSpec.from_json(_get_dict_spec(s_type, s_id))
    assert isinstance(spec, o_spec)
    assert isinstance(spec.placement, PlacementSpec)
    assert isinstance(spec.placement.hosts[0], HostPlacementSpec)
    assert spec.placement.hosts[0].hostname == 'host1'
    assert spec.placement.hosts[0].network == '1.1.1.1'
    assert spec.placement.hosts[0].name == ''
    assert spec.validate() is None
    ServiceSpec.from_json(spec.to_json())

def test_osd_unmanaged():
    osd_spec = {"placement": {"host_pattern": "*"},
                "service_id": "all-available-devices",
                "service_name": "osd.all-available-devices",
                "service_type": "osd",
                "spec": {"data_devices": {"all": True}, "filter_logic": "AND", "objectstore": "bluestore"},
                "unmanaged": True}

    dg_spec = ServiceSpec.from_json(osd_spec)
    assert dg_spec.unmanaged == True

def test_yaml():
    y = """service_type: crash
service_name: crash
placement:
  host_pattern: '*'
---
service_type: crash
service_name: crash
placement:
  host_pattern: '*'
unmanaged: true
---
service_type: rgw
service_id: default-rgw-realm.eu-central-1.1
service_name: rgw.default-rgw-realm.eu-central-1.1
placement:
  hosts:
  - ceph-001
networks:
- 10.0.0.0/8
- 192.168.0.0/16
spec:
  rgw_frontend_type: civetweb
  rgw_realm: default-rgw-realm
  rgw_zone: eu-central-1
---
service_type: osd
service_id: osd_spec_default
service_name: osd.osd_spec_default
placement:
  host_pattern: '*'
spec:
  data_devices:
    model: MC-55-44-XZ
  db_devices:
    model: SSD-123-foo
  filter_logic: AND
  objectstore: bluestore
  wal_devices:
    model: NVME-QQQQ-987
"""

    for y in y.split('---\n'):
        data = yaml.safe_load(y)
        object = ServiceSpec.from_json(data)

        assert yaml.dump(object) == y
        assert yaml.dump(ServiceSpec.from_json(object.to_json())) == y

@pytest.mark.parametrize("spec1, spec2, eq",
                         [
                             (
                                     ServiceSpec(
                                         service_type='mon'
                                     ),
                                     ServiceSpec(
                                         service_type='mon'
                                     ),
                                     True
                             ),
                             (
                                     ServiceSpec(
                                         service_type='mon'
                                     ),
                                     ServiceSpec(
                                         service_type='mon',
                                         service_id='foo'
                                     ),
                                     True
                             ),
                             # Add service_type='mgr'
                             (
                                     ServiceSpec(
                                         service_type='osd'
                                     ),
                                     ServiceSpec(
                                         service_type='osd',
                                     ),
                                     True
                             ),
                             (
                                     ServiceSpec(
                                         service_type='osd'
                                     ),
                                     DriveGroupSpec(),
                                     True
                             ),
                             (
                                     ServiceSpec(
                                         service_type='osd'
                                     ),
                                     ServiceSpec(
                                         service_type='osd',
                                         service_id='foo',
                                     ),
                                     False
                             ),
                             (
                                     ServiceSpec(
                                         service_type='rgw',
                                         service_id='foo',
                                     ),
                                     RGWSpec(service_id='foo'),
                                     True
                             ),
                         ])
def test_spec_hash_eq(spec1: ServiceSpec,
                      spec2: ServiceSpec,
                      eq: bool):

    assert (spec1 == spec2) is eq

@pytest.mark.parametrize(
    "s_type,s_id,s_name",
    [
        ('mgr', 's_id', 'mgr'),
        ('mon', 's_id', 'mon'),
        ('mds', 's_id', 'mds.s_id'),
        ('rgw', 's_id', 'rgw.s_id'),
        ('nfs', 's_id', 'nfs.s_id'),
        ('iscsi', 's_id', 'iscsi.s_id'),
        ('osd', 's_id', 'osd.s_id'),
    ])
def test_service_name(s_type, s_id, s_name):
    spec = ServiceSpec.from_json(_get_dict_spec(s_type, s_id))
    spec.validate()
    assert spec.service_name() == s_name

@pytest.mark.parametrize(
    's_type,s_id',
    [
        ('mds', 's:id'),
        ('rgw', '*s_id'),
        ('nfs', 's/id'),
        ('iscsi', 's@id'),
        ('osd', 's;id'),
    ])

def test_service_id_raises_invalid_char(s_type, s_id):
    with pytest.raises(SpecValidationError):
        spec = ServiceSpec.from_json(_get_dict_spec(s_type, s_id))
        spec.validate()
