import re
import subprocess


def render_prompt(conn, prompt_string):

    url = conn.engine.url

    user_name = url.username
    host = url.host
    if not url.port and conn.dialect.name in ("postgresql", "redshift"):
        port = 5432
    elif not url.port and conn.dialect.name == "snowflake":
        port = 443
    else:
        port = url.port

    if not host:
        host_name = "[local]"
    else:
        host_name = host

    if port is not None:
        port = str(port)
    else:
        port = ""

    database_name = url.database

    def replacer(match):
        if match.groups()[0] == "%n":
            return user_name
        elif match.groups()[0] == "%M":
            return host_name
        elif match.groups()[0] == "%>":
            return port
        elif match.groups()[0] == "%/":
            return database_name
        elif match.groups()[0].startswith("%`"):
            command = match.groups()[0]
            command = command[2:-1]

            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
            )

            value = result.stdout.decode("utf-8").strip()

            return value

    prompt = re.sub("(%([nM/>])|%(`.+?`))", replacer, prompt_string)

    return prompt
