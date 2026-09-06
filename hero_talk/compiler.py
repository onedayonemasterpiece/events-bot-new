"""Pure locked-token compilation. No model, storage, activation or browser permit.

Inputs are an internal normalized program and a trusted canonical resolver packet,
not arbitrary model JSON or MCP arguments. Semantic acceptance must bind the exact
input digest. Both placements consume the same compiled node contract.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import ipaddress
import json
import re
from typing import Any, Mapping
from urllib.parse import urlsplit

_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}')
_SHA = re.compile(r'[a-f0-9]{64}')
PLACEMENTS = frozenset({'home_hero', 'page_end'})
MEDIA_ROLES = frozenset({'event_photo', 'festival_image', 'editorial_image', 'capability_illustration'})
ORIGINS = frozenset({'system', 'catalog_signal', 'editorial_program', 'promo_campaign', 'user_state'})
PROGRAM_FIELDS = frozenset({'program_id','revision','origin','author_mode','placements','topic_anchor',
                            'chains','safe_until','dependencies','campaign_binding'})
PACKET_FIELDS = frozenset({'dependencies','facts','links','media'})
VERSION_FIELDS = frozenset({'schema','style','compiler','model_policy','prompt','renderer_min_version'})


class HeroCompileError(ValueError):
    pass


def _fail(reason):
    raise HeroCompileError(reason)


def _object(value, allowed, required=None):
    if not isinstance(value, dict) or set(value)-set(allowed) or set(required or ())-set(value):
        _fail('schema_invalid')
    return value


def _text(value, maximum=1000):
    # Never normalize, strip, add spaces or truncate owner Unicode content.
    if not isinstance(value,str) or not value or len(value)>maximum or any(ord(c)<32 and c not in '\n\t' for c in value):
        _fail('text_invalid')
    return value


def _id(value):
    if not isinstance(value,str) or not _ID.fullmatch(value):
        _fail('reference_invalid')
    return value


def _date(value):
    try:
        dt=datetime.fromisoformat(value.replace('Z','+00:00'))
        if dt.tzinfo is None:
            raise ValueError()
        return dt.astimezone(timezone.utc)
    except (AttributeError, TypeError, ValueError):
        _fail('deadline_invalid')


def _json(value):
    try:
        return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, UnicodeError):
        _fail('json_invalid')


def input_digest(program: Mapping, packet: Mapping, versions: Mapping) -> str:
    """Writer/reviewer/cache binding; no clocks, random IDs or private actor data."""
    return hashlib.sha256(_json({'program':program,'packet':packet,'versions':versions})).hexdigest()


def _href(value):
    value=_text(value,2000)
    if any(c.isspace() or ord(c)<32 for c in value) or '\\' in value:
        _fail('link_invalid')
    try:
        parsed=urlsplit(value)
        _ = parsed.port
    except ValueError:
        _fail('link_invalid')
    if value.startswith('/') and not value.startswith('//') and not parsed.scheme and not parsed.netloc:
        return value
    # Canonical resolver owns destination readiness. Compiler never fetches URLs.
    if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password:
        _fail('link_invalid')
    hostname=parsed.hostname.casefold()
    if hostname=='localhost' or hostname.endswith(('.localhost','.local','.internal')):
        _fail('link_invalid')
    try:
        address=ipaddress.ip_address(hostname)
    except ValueError:
        address=None
    if address is not None and not address.is_global:
        _fail('link_invalid')
    return value


@dataclass(frozen=True)
class CompiledHeroPack:
    content_sha256: str
    input_sha256: str
    json_bytes: bytes
    gzip_bytes: bytes

    def public(self) -> dict[str,Any]:
        return json.loads(self.json_bytes)


def compile_program(program: dict, packet: dict, versions: dict, *,
                    semantic_receipt: dict, now: datetime,
                    public_media_hosts: frozenset[str]=frozenset()) -> CompiledHeroPack:
    """Compile one immutable pack only after exact-input semantic acceptance.

    This validates syntax/reference/graph/expiry/media contracts, not factual
    truth, rendered overflow, live storage readiness or current campaign rights.
    Those remain resolver/reviewer/renderer/activation/live-permit gates.
    """
    _object(program,PROGRAM_FIELDS,PROGRAM_FIELDS-{'campaign_binding'})
    _object(packet,PACKET_FIELDS,PACKET_FIELDS)
    _object(versions,VERSION_FIELDS,VERSION_FIELDS)
    for value in versions.values():
        _text(value,120)
    if now.tzinfo is None:
        _fail('timezone_required')
    now=now.astimezone(timezone.utc)
    digest=input_digest(program,packet,versions)
    if semantic_receipt != {'input_sha256':digest,'verdict':'accept'}:
        _fail('semantic_acceptance_required')
    _id(program['program_id']); _id(program['topic_anchor'])
    revision=program['revision']
    if type(revision) is not int or not 1<=revision<=2**63-1:
        _fail('revision_invalid')
    if program['origin'] not in ORIGINS or program['author_mode'] not in {'automatic','assisted','verbatim'}:
        _fail('author_mode_invalid')
    placements=program['placements']
    if (not isinstance(placements,list) or not placements or len(placements)>2
            or any(p not in PLACEMENTS for p in placements) or len(set(placements))!=len(placements)):
        _fail('placement_invalid')
    campaign=program.get('campaign_binding')
    if program['origin']=='promo_campaign':
        _object(campaign,{'campaign_id','activity_id'}, {'campaign_id','activity_id'})
        if any(type(v) is not int or v<1 for v in campaign.values()):
            _fail('campaign_binding_invalid')
    elif campaign is not None:
        _fail('campaign_origin_mismatch')
    deadline=_date(program['safe_until'])
    if deadline<=now:
        _fail('expired')
    dependencies=program['dependencies']
    if not isinstance(dependencies,list) or len(dependencies)>64:
        _fail('dependencies_invalid')
    resolved=packet['dependencies']
    if not isinstance(resolved,dict):
        _fail('dependencies_invalid')
    used={}
    for dep in dependencies:
        _object(dep,{'ref','revision'}, {'ref','revision'})
        ref=_id(dep['ref']); _text(dep['revision'],200)
        if ref in used:
            _fail('dependency_duplicate')
        current=resolved.get(ref)
        if not isinstance(current,dict) or current.get('revision')!=dep['revision'] or current.get('eligible') is not True:
            _fail('dependency_incompatible')
        deadline=min(deadline,_date(current.get('eligible_until')))
        used[ref]=dep['revision']
    if deadline<=now:
        _fail('dependency_expired')
    def token(collection, ref):
        _id(ref)
        values=packet[collection]
        value=values.get(ref) if isinstance(values,dict) else None
        if not isinstance(value,dict) or value.get('dependency_ref') not in used:
            _fail('token_unbound')
        return value
    chains=program['chains']
    if not isinstance(chains,list) or not 1<=len(chains)<=8:
        _fail('chain_count_invalid')
    compiled=[]; chain_ids=set()
    for chain in chains:
        _object(chain,{'chain_id','nodes'}, {'chain_id','nodes'})
        chain_id=_id(chain['chain_id'])
        if chain_id in chain_ids:
            _fail('chain_duplicate')
        chain_ids.add(chain_id)
        nodes=chain['nodes']
        if not isinstance(nodes,list) or not 1<=len(nodes)<=3:
            _fail('node_count_invalid')
        ids=[]; output=[]
        for node in nodes:
            _object(node,{'node_id','fragments','next_node_id','media_ref','media_policy'}, {'node_id','fragments'})
            node_id=_id(node['node_id']); ids.append(node_id)
            fragments=node['fragments']
            if not isinstance(fragments,list) or not 1<=len(fragments)<=24:
                _fail('fragment_count_invalid')
            out=[]
            for fragment in fragments:
                _object(fragment,{'text','fact_token','link_token','label','accent','breakAfter'})
                kinds=set(fragment)&{'text','fact_token','link_token'}
                if len(kinds)!=1 or ('label' in fragment and 'link_token' not in fragment):
                    _fail('fragment_kind_invalid')
                current={}
                if 'text' in fragment:
                    current['text']=_text(fragment['text'])
                elif 'fact_token' in fragment:
                    current['text']=_text(token('facts',fragment['fact_token']).get('text'))
                else:
                    link=token('links',fragment['link_token'])
                    if link.get('ready') is not True:
                        _fail('route_not_ready')
                    current={'text':_text(fragment.get('label'),200), 'href':_href(link.get('href'))}
                for key in ('accent','breakAfter'):
                    if key in fragment:
                        if type(fragment[key]) is not bool:
                            _fail('fragment_style_invalid')
                        current[key]=fragment[key]
                out.append(current)
            if sum(len(f['text']) for f in out)>1000:
                _fail('node_text_too_long')
            item={'node_id':node_id,'fragments':out}
            if 'next_node_id' in node:
                item['next_node_id']=_id(node['next_node_id'])
            if 'media_policy' in node and node['media_policy'] not in {'required','optional_text_fallback'}:
                _fail('media_policy_invalid')
            if 'media_ref' in node:
                media=token('media',node['media_ref'])
                role=media.get('role')
                valid=(role in MEDIA_ROLES and media.get('public_verified') is True
                       and media.get('rights_verified') is True and media.get('geometry_verified') is True
                       and isinstance(media.get('sha256'),str) and _SHA.fullmatch(media['sha256'])
                       and media.get('geometry_sha256')==media['sha256'])
                if valid:
                    src=_href(media.get('src')); parsed=urlsplit(src)
                    valid=parsed.hostname in public_media_hosts and not parsed.query and not parsed.fragment
                if not valid:
                    if node.get('media_policy')!='optional_text_fallback':
                        _fail('media_not_ready')
                else:
                    item['media']={'asset_ref':_id(node['media_ref']), 'role':role,'src':src,
                                   'sha256':media['sha256'],'alt':_text(media.get('alt'),300)}
                    if role in {'event_photo','festival_image'}:
                        item['media']['canonical_ref']=_id(media.get('canonical_ref'))
                        if item['media']['canonical_ref'] not in used:
                            _fail('media_entity_unbound')
            elif 'media_policy' in node:
                _fail('media_policy_without_media')
            output.append(item)
        if len(set(ids))!=len(ids):
            _fail('node_duplicate')
        # Initial chain is a complete forward path; no unreachable nodes or cycles.
        for index,item in enumerate(output):
            expected=ids[index+1] if index+1<len(ids) else None
            if item.get('next_node_id')!=expected:
                _fail('chain_graph_invalid')
        compiled.append({'chain_id':chain_id,'nodes':output})
    public={'schema_version':versions['schema'],'renderer_min_version':versions['renderer_min_version'],
            'program_id':program['program_id'],'revision':revision,'origin':program['origin'],
            'author_mode':program['author_mode'],'placements':placements,'topic_anchor':program['topic_anchor'],
            'safe_until':deadline.isoformat(),'dependencies':[{'ref':k,'revision':v} for k,v in sorted(used.items())],
            'chains':compiled, 'input_sha256':digest}
    if campaign is not None:
        public['campaign_binding']=campaign
    raw=_json(public); compressed=gzip.compress(raw,mtime=0)
    if len(compressed)>32*1024:
        _fail('pack_budget_exceeded')
    return CompiledHeroPack(hashlib.sha256(raw).hexdigest(),digest,raw,compressed)
