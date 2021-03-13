Basic redisent Example
======================

This portion of the documentation covers a rather basic but illustrative example which stores a simple time-based reminder in a Redis hash key.

Here is a rather straight forward example of a ``Reminder`` Redis hash entry which stores a few basic attributes with it:

.. literalinclude:: ../examples/reminder.py
   :language: python
   :linenos:

Creating a Reminder instance
----------------------------
Next, we can use ``IPython`` to create a :py:class:`redisent.helpers.RedisentHelper` instance (for simplicity, this will be non-async) and create an instance of the ``Reminder`` class, persist it to Redis and then fetch it back. Along the way, the intrinsic value of ``dataclasses`` will become apparent in the form of auto-generated ``__repr__``, ``__init__`` and ``__str__`` methods.

.. note::
   Invoke the ``ipython`` command from the same directory as a file named ``reminder.py``. This will result in ``from reminder import Reminder`` auto-magically working.

.. code-block:: ipython

   In [1]: from reminder import Reminder
      ...: from redisent import RedisentHelper
      ...:
      ...: from datetime import datetime, timedelta
   [2021-01-07 18:36:09,101] DEBUG in __init__: Logging started for redisent.

   In [2]: dt_now = datetime.now()
      ...: trig_dt = dt_now + timedelta(minutes=5)

   In [3]: rh = RedisentHelper(use_async=False)

   In [4]: rem = Reminder(redis_id='reminders', member_id=12345, member_name='Jon',
      ...:                channel_id=54321, channel_name='#test',
      ...:                provided_when='in 5 minutes',
      ...:                content='Test Reminder',
      ...:                trigger_ts=trig_dt.timestamp(),
      ...:                created_ts=dt_now.timestamp())
  
Here, the ``rem`` object will be an instance of ``Reminder`` with the corrosponding attributes provided to the ``ctor`` for ``Reminder``. All ``RedisEntry`` subclassess have a :py:meth:`redisent.models.RedisEntry.dump` method which will dump a string representation of the object:

.. code-block:: ipython

   In [5]: print(rem.dump())

   RedisEntry (Reminder) for key "reminders", hash entry "12345:1610323201.801648":
   => member_id    -> "12345"
   => member_name    -> "Jon"
   => channel_id    -> "54321"
   => channel_name    -> "#test"
   => provided_when    -> "in 5 minutes"
   => content    -> "Test Reminder"
   => trigger_ts    -> "1610323201.801648"
   => created_ts    -> "1610322901.801648"
   => is_complete    -> "False"

Storing an entry in Redis
-------------------------
Next using the already created instance of :py:class:`redisent.helpers.RedisentHelper`, ``rh``, the :py:meth:`redisent.models.RedisEntry.store` method can be used to first store the reminder in Redis. Here, the Redis key will be presumed to be ``reminders``.

.. code-block:: ipython

   In [6]: rem.store(rh)
   Out[6]: 1

   In [12]: with rh.wrapped_redis(op_name='hkeys("reminders")') as r_conn:
       ...:     rem_keys = r_conn.hkeys('reminders')
       ...:

   In [13]: rem_keys
   Out[13]: [b'12345:1610323201.801648']

By using the :py:func:`redisent.helpers.RedisentHelper.wrapped_redis` context manager, a new Redis connection is created for calling ``hkeys("reminders")`` and finally the keys are dumped out (with no encoding, hence the result being represented as ``bytes``. Using the Redis command ``hget("reminders", "12345:1610323201.801648")`` will similarly return the ``bytes``-encoded blob representing the ``Reminder`` class within Redis.

Retrieving an entry from Redis
------------------------------
Finally, we can fetch back the original reminder from Redis using :py:meth:`redisent.models.RedisEntry.fetch`:

.. code-block:: ipython

   In [14]: rem_fetched = Reminder.fetch(helper=rh, redis_id='reminders', redis_name='12345:1610323201.801648')

   In [15]: rem_fetched
   Out[15]: Reminder(redis_id='reminders', redis_name='12345:1610323201.801648', member_id=12345, member_name='Jon', channel_id=54321, channel_name='#test', provided_when='in 5 minutes', content='Test Reminder', trigger_ts=1610323201.801648, created_ts=1610322901.801648, is_complete=True)

   In [16]: print(rem_fetched.dump())

   RedisEntry (Reminder) for key "reminders", hash entry "12345:1610323201.801648":
   => member_id    -> "12345"
   => member_name    -> "Jon"
   => channel_id    -> "54321"
   => channel_name    -> "#test"
   => provided_when    -> "in 5 minutes"
   => content    -> "Test Reminder"
   => trigger_ts    -> "1610323201.801648"
   => created_ts    -> "1610322901.801648"
   => is_complete    -> "True"

   In [17]: rem_fetched == rem
   Out[17]: True

Et voila! An equivilent instance of the newly created instance of ``Reminder``, ``rem`` was fetched and de-serialized as ``rem_fetched``.

Wrapping Up
-----------
The astute reader will notice the requirement for providing ``redis_id`` with a static value ("reminders" in this case). If a ``RedisEntry`` subclass should always use a specific key, it is often easier to re-define the ``redis_id`` dataclass :py:func:`dataclasses.field`.

Here is what it would look like in the case of the ``Reminder`` class:

.. code-block:: python
   :linenos:
   :emphasize-lines: 9,10

   @dataclass
   class Reminder(RedisEntry):
       member_id: str = field(default_factory=str)
       member_name: str = field(default_factory=str)

       channel_id: str = field(default_factory=str)
       channel_name: str = field(default_factory=str)

       # Force "redis_id" to be "reminders"
       redis_id: str = field(default='reminders', metadata={'redis_field': True})


