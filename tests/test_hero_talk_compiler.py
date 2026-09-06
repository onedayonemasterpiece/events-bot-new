"""Pure internal compiler fixtures are not content approval or live serving."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json

import pytest

from hero_talk.compiler import HeroCompileError, compile_program, input_digest

NOW=datetime(2026,9,6,12,tzinfo=timezone.utc)
VERSIONS=dict(schema='hero-talk-pack-v1',style='fixture-style',compiler='v1',
              model_policy='no-model-fixture',prompt='fixture',renderer_min_version='hero-talk-shared-v1')


def fixture():
    program={'program_id':'fixture-program','revision':1,'origin':'editorial_program','author_mode':'verbatim',
             'placements':['home_hero','page_end'],'topic_anchor':'fixture-weekend',
             'safe_until':'2026-09-07T00:00:00+00:00',
             'dependencies':[{'ref':'route:weekend','revision':'r1'}],
             'chains':[{'chain_id':'fixture-chain','nodes':[{'node_id':'fixture-node','fragments':[
                 {'text':'«Точно так».\u00a0 '}, {'link_token':'weekend','label':'Выходные','accent':True}]}]}]}
    packet={'dependencies':{'route:weekend':{'revision':'r1','eligible':True,'eligible_until':'2026-09-06T23:00:00Z'}},
            'facts':{},'links':{'weekend':{'href':'/weekend/','ready':True,'dependency_ref':'route:weekend'}},'media':{}}
    return program,packet


def compile_fixture(program,packet,**kwargs):
    versions=kwargs.pop('versions',VERSIONS)
    return compile_program(program,packet,versions,semantic_receipt={
        'input_sha256':input_digest(program,packet,versions),'verdict':'accept'},now=NOW,**kwargs)


def test_exact_unicode_shared_placement_contract_and_deterministic_bytes():
    program,packet=fixture()
    first=compile_fixture(program,packet); second=compile_fixture(program,packet)
    assert first==second
    result=first.public()
    fragments=result['chains'][0]['nodes'][0]['fragments']
    assert ''.join(f['text'] for f in fragments)=='«Точно так».\u00a0 Выходные'
    assert fragments[1]['href']=='/weekend/'
    assert result['placements']==['home_hero','page_end']
    assert result['safe_until']=='2026-09-06T23:00:00+00:00'
    assert first.content_sha256==hashlib.sha256(first.json_bytes).hexdigest()
    assert 'event' not in result and 'campaign_binding' not in result
    assert not {'actor_subject','private_brief','semantic_receipt'} & set(result)


@pytest.mark.parametrize('mutation',[
    lambda p,k:p.update(actor_subject='private-owner'),
    lambda p,k:p.update(revision=True),
    lambda p,k:p.update(placements=['service_cta']),
    lambda p,k:p.update(safe_until='2026-09-01T00:00:00Z'),
    lambda p,k:p['dependencies'][0].update(revision='stale'),
    lambda p,k:k['dependencies']['route:weekend'].update(eligible=False),
    lambda p,k:k['dependencies']['route:weekend'].update(eligible_until='2026-09-06T11:00:00Z'),
    lambda p,k:k['links']['weekend'].update(ready=False),
    lambda p,k:k['links']['weekend'].update(href='javascript:alert(1)'),
    lambda p,k:k['links']['weekend'].update(href='//evil.test/path'),
    lambda p,k:k['links']['weekend'].update(href='https://user:secret@example.test/path'),
    lambda p,k:k['links']['weekend'].update(dependency_ref='unbound'),
    lambda p,k:p['chains'][0]['nodes'][0].update(next_node_id='fixture-node'),
    lambda p,k:p['chains'][0]['nodes'][0]['fragments'][0].update(onclick='evil()'),
    lambda p,k:p.update(campaign_binding={'campaign_id':1,'activity_id':2}),
    lambda p,k:p.update(origin='promo_campaign'),
])
def test_unready_or_invalid_contract_fails_closed(mutation):
    program,packet=fixture(); mutation(program,packet)
    with pytest.raises(HeroCompileError): compile_fixture(program,packet)


def test_acceptance_bound_to_exact_copy_packet_and_policy_version():
    program,packet=fixture()
    receipt={'input_sha256':input_digest(program,packet,VERSIONS),'verdict':'accept'}
    for changed_program,changed_packet,versions in (
        ({**program,'topic_anchor':'changed'},packet,VERSIONS),
        (program,{**packet,'facts':{'new':{'text':'changed'}}},VERSIONS),
        (program,packet,{**VERSIONS,'style':'changed'}),
    ):
        with pytest.raises(HeroCompileError,match='semantic_acceptance_required'):
            compile_program(changed_program,changed_packet,versions,semantic_receipt=receipt,now=NOW)


def test_locked_fact_tokens_resolve_without_mutating_literal_text():
    program,packet=fixture()
    program['dependencies'].append({'ref':'event:123','revision':'e1'})
    packet['dependencies']['event:123']={'revision':'e1','eligible':True,'eligible_until':'2026-09-06T20:00:00Z'}
    packet['facts']['price']={'text':'500 ₽','dependency_ref':'event:123'}
    program['chains'][0]['nodes'][0]['fragments']=[{'text':'Цена: '},{'fact_token':'price'}]
    before=deepcopy(program)
    result=compile_fixture(program,packet).public()
    assert ''.join(f['text'] for f in result['chains'][0]['nodes'][0]['fragments'])=='Цена: 500 ₽'
    assert program==before


def with_media(program,packet):
    program['chains'][0]['nodes'][0].update(media_ref='fixture-image')
    packet['media']['fixture-image']={'dependency_ref':'route:weekend','role':'editorial_image',
        'public_verified':True,'rights_verified':True,'geometry_verified':True,
        'sha256':'a'*64,'pixel_sha256':'b'*64,'geometry_pixel_sha256':'b'*64,'src':'https://cdn.example.test/aa.webp','alt':'Fixture editorial illustration'}


def test_editorial_media_needs_no_fake_event_and_exact_sha_geometry():
    program,packet=fixture();with_media(program,packet)
    result=compile_fixture(program,packet,public_media_hosts=frozenset({'cdn.example.test'})).public()
    media=result['chains'][0]['nodes'][0]['media']
    assert media['role']=='editorial_image' and 'canonical_ref' not in media
    assert media['sha256']=='a'*64 and media['pixel_sha256']=='b'*64
    packet['media']['fixture-image']['geometry_pixel_sha256']='c'*64
    with pytest.raises(HeroCompileError,match='media_not_ready'):
        compile_fixture(program,packet,public_media_hosts=frozenset({'cdn.example.test'}))
    program['chains'][0]['nodes'][0]['media_policy']='optional_text_fallback'
    assert 'media' not in compile_fixture(program,packet).public()['chains'][0]['nodes'][0]


@pytest.mark.parametrize('patch',[
    {'rights_verified':False},{'public_verified':False},{'src':'https://unapproved.test/image.webp'},
    {'src':'https://cdn.example.test/image?token=private'},{'role':'private_attachment'},
    {'role':'event_photo','canonical_ref':'event:999'},
])
def test_private_or_unbound_media_cannot_enter_public_pack(patch):
    program,packet=fixture();with_media(program,packet)
    packet['media']['fixture-image'].update(patch)
    with pytest.raises(HeroCompileError):
        compile_fixture(program,packet,public_media_hosts=frozenset({'cdn.example.test'}))


def test_campaign_binding_cannot_be_relabelled_editorial():
    program,packet=fixture()
    program.update(origin='promo_campaign',campaign_binding={'campaign_id':1,'activity_id':2})
    result=compile_fixture(program,packet).public()
    assert result['campaign_binding']=={'campaign_id':1,'activity_id':2}
    # It is still just a pack, not an authorization or independently active campaign.
    assert not {'campaign_status','budget','permit','active'} & set(result)
    program['origin']='editorial_program'
    with pytest.raises(HeroCompileError,match='campaign_origin_mismatch'):
        compile_fixture(program,packet)
