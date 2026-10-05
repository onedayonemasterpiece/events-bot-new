import base64
import re
from urllib.parse import urlencode, parse_qs, urlsplit
from private_events_mcp.crypto import pkce_s256


async def login(client, cfg, scope="events:read events:write"):
    verifier = "h" * 64
    callback = "https://chatgpt.com/connector/oauth/test-callback-id"
    page = await client.get(
        cfg.oauth_authorize_path
        + "?"
        + urlencode(
            dict(
                response_type="code",
                client_id=cfg.oauth_client_id,
                redirect_uri=callback,
                state="owner-fixture",
                resource=cfg.resource,
                scope=scope,
                code_challenge=pkce_s256(verifier),
                code_challenge_method="S256",
            )
        )
    )
    assert page.status == 200
    sealed = re.search(
        r'name="authorization_request" value="([^"]+)"', await page.text()
    )[1]
    grant = await client.post(
        cfg.oauth_authorize_path,
        data={"authorization_request": sealed, "operator_token": cfg.operator_token},
        allow_redirects=False,
    )
    assert grant.status == 302
    code = parse_qs(urlsplit(grant.headers["Location"]).query)["code"][0]
    basic = base64.b64encode(
        f"{cfg.oauth_client_id}:{cfg.oauth_client_secret}".encode()
    ).decode()
    response = await client.post(
        cfg.oauth_token_path,
        data=dict(
            grant_type="authorization_code",
            code=code,
            redirect_uri=callback,
            resource=cfg.resource,
            code_verifier=verifier,
        ),
        headers={"Authorization": "Basic " + basic},
    )
    assert response.status == 200
    return (await response.json())["access_token"]
