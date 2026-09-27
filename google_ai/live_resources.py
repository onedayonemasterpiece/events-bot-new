"""KenigEvents shared Live resource boundary; ordinary GoogleAIClient is unchanged.

The caller is a trusted, authenticated backend handler, not a public model tool.
This module deliberately does not add a browser endpoint or another voice stack.
"""
from __future__ import annotations
import hashlib
import re
from typing import Any,Callable,Mapping

RESOURCE_ENV_NAMES = (
    'AI_RESOURCE_CONTROL_URL',
    'AI_RESOURCE_CONTROL_SERVICE_KEY',
    'GOOGLE_AI_LIMITER_SUPABASE_URL',
    'GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY',
    'AI_RESOURCE_LEDGER_ID',
)

def live_resource_environment(environment:Mapping[str,str]) -> dict[str,str]:
    """Expose only shared-authority configuration to the Live controller.

    Provider keys from the host environment are deliberately excluded. If the
    shared authority is unavailable, KenigEvents Live fails closed instead of
    switching to a local provider credential.
    """
    return {
        name: environment[name]
        for name in RESOURCE_ENV_NAMES
        if isinstance(environment.get(name),str) and environment[name].strip()
    }

async def run_live_search(*,authorized_session_id:str,environment:Mapping[str,str],
                          reader:Any,on_event:Callable[[dict],None]) -> None:
    if not isinstance(authorized_session_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,128}',authorized_session_id):
        raise ValueError('authorized_session_id_required')
    try:
        from ai_resource_control import run_guarded
    except ImportError:
        on_event({'type':'error','code':'RESOURCE_PACKAGE_MISSING','message':'RESOURCE_PACKAGE_MISSING'})
        return
    binding=hashlib.sha256(authorized_session_id.encode()).hexdigest()
    await run_guarded(consumer='kenigevents',environment=live_resource_environment(environment),reader=reader,
                      on_event=on_event,binding=binding)
