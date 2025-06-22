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
