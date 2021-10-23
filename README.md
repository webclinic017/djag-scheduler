# Djag Scheduler

**(Dj)ango Task D(AG) (Scheduler)**

## Overview

- Djag scheduler associates scheduling information with celery tasks
- The task schedule is persisted in the database using Django ORM
- Djag scheduler allows for defining task dependencies
- Djag scheduler can handle misfire events
- The schedules can be managed through the Django admin interface

## Quick Setup

1. Install djag-scheduler from [``pypi``](https://pypi.org/project/djag-scheduler)

    ```shell
    pip install djag-scheduler
    ```

2. Add [``djag_scheduler``](https://github.com/m0hithreddy/djag-scheduler),
   [``timezone_field``](https://github.com/mfogel/django-timezone-field),
   [``django_json_widget``](https://github.com/jmrivas86/django-json-widget) (installed by default)
   to the Django INSTALLED_APPS

   ```python
   INSTALLED_APPS = [
        ...,
        
        # Djag Scheduler Apps
        'timezone_field',
        'django_json_widget',
        'djag_scheduler'
   ]
   ```

3. Setup [``Django CACHE``](https://docs.djangoproject.com/en/dev/topics/cache/#setting-up-the-cache).
   You can choose any cache backend. But it is important to configure ``'djag_scheduler'`` cache. For example:
   
   ```python
   CACHES = {
       'default': {
           'BACKEND': 'django.core.cache.backends.memcached.PyLibMCCache',
           'LOCATION': '127.0.0.1:11211',
       },
        
       # Djag Scheduler cache
       'djag_scheduler': {
           'BACKEND': 'django.core.cache.backends.memcached.PyLibMCCache',
           'LOCATION': '127.0.0.1:11211',
           'TIMEOUT': None
       }
   }
   ```
   
   > **Caution**: [Local-memory caching](https://docs.djangoproject.com/en/dev/topics/cache/#local-memory-caching)
   > will not work. See [Case for Django Cache](#case-for-django-cache)
   
4. It is highly recommended to configure [``Django Timezone``](https://docs.djangoproject.com/en/dev/ref/settings/#use-tz)
   since djag-scheduler relies on Django ORM.

   ```python
   USE_TZ = True
   ```

5. [Optional Configurations](#optional-configurations) (In Django project settings).
   
   ```python
   ...
   
   # Djag Scheduler Configuration
   [CELERY_NAMESPACE]_TIMEZONE = 'UTC'
   DJAG_DEFAULT_INTERVAL = 60
   DJAG_SCHEDULE_CHECK_INTERVAL = 300
   DJAG_TASK_ESTIMATED_RUN_TIME = 60
   DJAG_RESILIENT_SYNC_INTERVAL = 600
   ```

6. Run migrations

   ```shell
   python manage.py migrate
   ```

Now you can run the server, navigate to the Django admin interface and start creating schedules.

> **Note:** This is the configuration for djag-scheduler, and this alone will not run the tasks. 
> See [Running Celery Services](#running-celery-services) for task execution

## Running Celery Services

Djag Scheduler provides a [custom scheduler](https://docs.celeryproject.org/en/stable/userguide/periodic-tasks.html#using-custom-scheduler-classes)
which can be used with celery services.

1. [Configure celery](https://docs.celeryproject.org/en/stable/django/first-steps-with-django.html)
   in your Django project and add [celery tasks](https://docs.celeryproject.org/en/stable/django/first-steps-with-django.html#using-the-shared-task-decorator)
   

2. Run celery worker service from the Django project root

   ```shell
   celery -A [project_name] worker --loglevel=info
   ```
   
3. Run celery beat service

   ```shell
   celery -A [project_name] beat -l info --scheduler djag_scheduler.scheduler:DjagScheduler 
   ```

Djag Scheduler will fetch the schedule from the database and starts executing the tasks. Both worker and 
beat services can be started in [one process](https://django-celery-beat.readthedocs.io/en/latest/#example-running-periodic-tasks).
Services can be [daemonized](https://docs.celeryproject.org/en/stable/userguide/daemonizing.html) as well

## Optional Configurations

- [[CELERY_NAMESPACE]_TIMEZONE](https://docs.celeryproject.org/en/stable/django/first-steps-with-django.html):
  Djag uses the same default timezone the celery is configured to use (Timezone can be configured per crontab).
  Default: UTC
  
- DJAG_DEFAULT_INTERVAL: Djag scheduler's default max loop interval. Default (sec): 60 

- DJAG_SCHEDULE_CHECK_INTERVAL: The interval at which djag checks for schedule changes. In the interim schedule 
  is considered unchanged. Default (sec): 300
  
- DJAG_TASK_ESTIMATED_RUN_TIME: Hint djag-scheduler about task execution period. Default (sec): 60 

- DJAG_RESILIENT_SYNC_INTERVAL: The scheduling stats of a task are synced to the database at each task activation 
  and deactivation. This option is there to build resiliency if the past syncs fail. Default (sec): 600

> **Note:** Almost everyone should configure these options according to their needs instead of relying on defaults.
> Let's consider you have a task with Skip Misfires, and the following configuration: **Estimated Run Time > Default Interval 
> \> Task's Grace Period**. Djag scheduler might end up skipping the task just because you hinted at it with a higher estimate.
> In general, increase your grace periods with higher runtime estimates.

## Dependency Resolution

Djag Scheduler at the core builds [Task DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) for dependency management.
But due to the addition of crontab information, djag-scheduler resolves dependencies slightly in a different way. 
The following conditions are to be met for the task (depender) to execute:

- There should be a task (depender) crontab event pending execution (on-time, delayed, coalesced, ...).

- For each dependency, there should be at least one new executed dependee event (on-time, delayed, coalesced, ...) since 
  the depender's last event execution start time.

Sometimes we might need to hold for the depender's execution (Ex: consume whole output) to execute the next version of the
dependee. Such dependencies can be defined by checking the [``Future Depends``](#admin-interface) flag. The future dependencies 
differ in the first time execution. For the first time, dependence is assumed to be resolved irrespective of the dependee's 
status, and the subsequent versions follow the same rules as described above.

## Admin Interface

Most options are straightforward. The following listed options might require some attention:

### Crontabs

- Crontabs are managed through [``croniter``](https://github.com/taichino/croniter), and timezones through [``django-timezone-field``](https://github.com/mfogel/django-timezone-field)

- By default, timezone is set to [``[CELERY_NAMESPACE]_TIMEZONE``](https://docs.celeryproject.org/en/stable/django/first-steps-with-django.html) 
  else ``UTC``.
  
### Periodic Tasks

- Grace Period: Number of seconds from the actual cron value for the task to be categorized as a misfire

- Skip Misfires: Skip all the misfire events

- Coalesce Misfires: Run one event (latest among misfires) for all the misfires
  
- Cron Base: Initial time for evaluating crontab. Time displayed will be in Django's [``TIME_ZONE``](https://docs.djangoproject.com/en/3.2/ref/settings/#time-zone).
  Cron base is set to current time by default.

- Task's ``*args or **kwargs`` can be set under ``Arguments`` section. The rendered JSON widget 
  comes from [``django-json-widget``](https://github.com/jmrivas86/django-json-widget)

### Task Dependencies

- Future Depends: The first version is independent, but the future versions of the depender depends on the dependee.
  See [``Dependency Resolution``]()
  
### User Actions

- The idea is to provide a mechanism for creating actions, which serves as control signals for running tasks.
  For generic purposes, only ``Unclassified Action`` is added to the drop-down. If you are extending this project,
  you can add actions and create forms per action (See [``user_action_forms``](https://github.com/m0hithreddy/djag-scheduler/blob/main/djag_scheduler/forms/user_action_forms.py)).

- Some internal actions are created for djag's functioning.

### User Action Audits

- Audit for [``User Actions``](#user-actions)

## Case for Django Cache

Djag Scheduler requires the task status for proper dependency resolution. This can be communicated from the worker 
service either by configuring the celery result backend or by using celery [``signals``](https://docs.celeryproject.org/en/stable/userguide/signals.html#task-postrun)
with Django cache as a bridge between worker and beat services. For now, the Django cache solution is adopted. 
For the same reason, cache backends like [``Local-memory caching``](https://docs.djangoproject.com/en/dev/topics/cache/#local-memory-caching) 
whose cache store is unique to Django instance will not work.

## Initial Credits

[``djag-scheduler``](https://github.com/m0hithreddy/djag-scheduler) started as an extended version of
[``django-celery-beat``](https://github.com/celery/django-celery-beat), providing support for task dependencies and
resiliency. ``django-celery-beat`` served as a reference in writing a database-backed custom ``Scheduler`` for celery-beat 
service. For more information see, [``LICENSE``](https://github.com/m0hithreddy/djag-scheduler/blob/main/LICENSE)