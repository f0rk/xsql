from sqlalchemy.engine.url import make_url


def resolve_arn(arn):
    if arn.startswith("arn:aws:ssm:"):
        return resolve_ssm(arn)
    elif arn.startswith("arn:aws:secretsmanager:"):
        return resolve_secretsmanager(arn)
    else:
        raise ValueError("unable to handle {}".format(arn))


def get_region(arn):
    arn_parts = arn.split(":")
    region = arn_parts[3]

    if not region or region == "":
        return None

    return region


def get_name(arn):
    arn_parts = arn.split(":")
    name = arn_parts[-1]

    if "parameter" not in name:
        name = ":".join([arn_parts[-2], arn_parts[-1]])

    # remove parameter/ prefix
    if name.startswith("parameter/"):
        name = name[10:]

    name = "/" + name

    return name


def resolve_ssm(arn):
    import botocore.session

    bs = botocore.session.get_session()
    region_name = get_region(arn)
    ssm_client = bs.create_client(
        "ssm",
        region_name=region_name,
    )

    name = get_name(arn)

    resp = ssm_client.get_parameter(
        Name=name,
        WithDecryption=True,
    )

    return resp["Parameter"]["Value"]


def resolve_secretsmanager(arn):
    import botocore.session

    bs = botocore.session.get_session()
    region_name = get_region(arn)
    secretsmanager_client = bs.create_client(
        "secretsmanager",
        region_name=region_name,
    )

    resp = secretsmanager_client.get_secret_value(SecretId=arn)

    return resp["SecretString"]


def rds_auth(url):
    import botocore.session

    url = make_url(url)

    host_parts = url.host.split(".")
    cluster_region = host_parts[2]

    bs = botocore.session.get_session()
    rds_client = bs.create_client(
        service_name="rds",
        region_name=cluster_region,
    )

    password = rds_client.generate_db_auth_token(
        DBHostname=url.host,
        Port=url.port,
        DBUsername=url.username,
    )

    url = url.set(password=password)
    url = url.update_query_dict({"password": password})

    return url.render_as_string(hide_password=False)


def redshift_auth(url):
    import botocore.session

    url = make_url(url)

    host_parts = url.host.split(".")
    cluster_region = host_parts[2]
    cluster_identifier = host_parts[0]

    bs = botocore.session.get_session()
    redshift_client = bs.create_client(
        service_name="redshift",
        region_name=cluster_region,
    )

    redshift_response = redshift_client.get_cluster_credentials(
        DbUser=url.username,
        DbName=url.database,
        ClusterIdentifier=cluster_identifier,
        AutoCreate=False,
    )

    url = url.set(username=redshift_response["DbUser"])
    url = url.set(password=redshift_response["DbPassword"])

    url = url.update_query_dict({"password": redshift_response["DbPassword"]})

    return url.render_as_string(hide_password=False)
