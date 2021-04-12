from __future__ import annotations

import redis

from typing import Mapping, Any, Optional


class RedisError(Exception):
    """
    Exception class for Redisent-raised errors

    Exceptions of this type will be raised for almost any errors encountered within ``redisent``

    This class also offers the :py:func`redisent.errors.RedisError.dump` method which can be used to build a verbose blurb about the
    error and available context
    """

    message: str  #: Error Message

    base_exception: Optional[Exception]  #: Optional base :py:exc:`Exception` raise prior to this error
    related_command: Optional[str]       #: The Redis command or ``op_name`` that caused this error (if available)
    extra_attrs: Mapping[str, Any] = {}  #: Mapping of optional contextual attributes related to this error

    @property
    def is_connection_error(self) -> bool:
        """
        Indicates if the underlying error was related to a connection failure

        If ``base_exception`` is an instance of ``redis.exceptions.ConnectionError``, this will return ``True``
        """

        return True if self.base_exception and isinstance(self.base_exception, redis.exceptions.ConnectionError) else False

    def __init__(self, message: str, base_exception: Exception = None, related_command: str = None, extra_attrs: Mapping[str, Any] = None) -> None:
        """
        Build a new ``RedisError`` exception with optionally provided contextual details

        :param message: the error message
        :param base_exception: an optional base exception related to this one
        :param related_command: if available, the related ``op_name`` / Redis command that raised this error (otherwise this will be "N/A")
        :param extra_attrs: optional mapping of related values at the time of the exception being raised
        """

        super(RedisError, self).__init__(message)

        self.message = message
        self.base_exception = base_exception
        self.related_command = related_command
        self.extra_attrs = extra_attrs or {}

    def __repr__(self) -> str:
        repr_out = f'<RedisError(message="{self.message}"'

        if self.base_exception:
            repr_out = f'{repr_out}, base_exception="{self.base_exception}"'

        if self.extra_attrs:
            ext_attrs = '", "'.join([f'{attr} -> {val}' for attr, val in self.extra_attrs.items()])
            repr_out = f'{repr_out}, extra_attrs="{ext_attrs}"'

        return f'{repr_out})>'

    def __str__(self) -> str:
        str_out = 'Redis Error '

        if self.related_command:
            str_out = f'{str_out}with command "{self.related_command}"'

        return f'{str_out}: {self.message}'

    def dump(self) -> str:
        """
        Helper method for building a verbose textual representation of the error and any available context at the time
        of the exception being raised
        """

        dump_out = str(self) + '\n'

        if self.base_exception:
            dump_out += f'-> Base Error:\t"{self.base_exception}\n"'

        if self.extra_attrs:
            dump_out += 'Extra Context:\n'
            for ex_key, ex_val in self.extra_attrs.items():
                dump_out += f'= "{ex_key}"\t-> "{ex_val}"'

        return dump_out
