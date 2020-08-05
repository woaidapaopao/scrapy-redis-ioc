import six

from scrapy.utils.misc import load_object

from . import defaults


# Shortcut maps 'setting name' -> 'parmater name'.
SETTINGS_PARAMS_MAP = {
    'REDIS_URL': 'url',
    'REDIS_HOST': 'host',
    'REDIS_PORT': 'port',
    'REDIS_ENCODING': 'encoding',
}

# 集群连接配置
CLUSTER_SETTINGS_PARAMS_MAP = {
    'REDIS_CLUSTER_URL': 'url',
    'STARTUP_NODES': 'startup_nodes',
    'REDIS_ENCODING': 'encoding',
}


def get_redis_from_settings(settings):
    """Returns a redis client instance from given Scrapy settings object.

    This function uses ``get_client`` to instantiate the client and uses
    ``defaults.REDIS_PARAMS`` global as defaults values for the parameters. You
    can override them using the ``REDIS_PARAMS`` setting.

    """
    params = defaults.REDIS_PARAMS.copy()
    params.update(settings.getdict('REDIS_PARAMS'))
    if 'redis_cls' in params:
        redis_cls = params.pop('redis_cls', defaults.REDIS_CLS)
        params['redis_cls'] = load_object(redis_cls)
        get_params(params, SETTINGS_PARAMS_MAP, settings)
        return get_redis(**params)
    elif 'redis_cluster_cls' in params:
        redis_cluster_cls = params.pop('redis_cluster_cls', defaults.REDIS_CLUSTER_CLS)
        params['redis_cluster_cls'] = load_object(redis_cluster_cls)
        get_params(params, CLUSTER_SETTINGS_PARAMS_MAP, settings)
        return get_redis(**params)
    else:
        raise AttributeError("redis_cls or redis_cluster_cls cannot find")


def get_params(params, setting_params_map, settings):
    for source, dest in setting_params_map.items():
        val = settings.get(source)
        if val:
            params[dest] = val
    return params


def get_redis(**kwargs):
    """Returns a redis client instance.

    Parameters
    ----------
    redis_cls : class, optional
        Defaults to ``redis.StrictRedis``.
    url : str, optional
        If given, ``redis_cls.from_url`` is used to instantiate the class.
    **kwargs
        Extra parameters to be passed to the ``redis_cls`` class.

    Returns
    -------
    server
        Redis client instance.

    """
    if 'redis_cluster_cls' in kwargs:
        redis_cls = kwargs.pop('REDIS_CLUSTER_CLS', defaults.REDIS_CLUSTER_CLS)
    elif 'redis_cls' in kwargs:
        redis_cls = kwargs.pop('REDIS_CLS', defaults.REDIS_CLUSTER_CLS)
    else:
        raise AttributeError("redis_cls or redis_cluster_cls cannot find")
    url = kwargs.pop('url', None)
    if url:
        return redis_cls.from_url(url, **kwargs)
    else:
        return redis_cls(**kwargs)


from_setting = get_redis_from_settings