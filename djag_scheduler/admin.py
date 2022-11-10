""" Djag Admin interface."""

from django.contrib import admin
from django.db import models
from django.template.defaultfilters import pluralize
from django.urls import path
from django.template.response import TemplateResponse
from django.core.exceptions import SuspiciousOperation

from djag_scheduler.models import (
    PeriodicTask, CrontabSchedule, TaskDependency,
    UserAction, UserActionAudit
)
import djag_scheduler.models.user_action_model as action_choices
from djag_scheduler.forms import (
    UserActionSelectForm, PeriodicTaskForm,
    get_action_form, UserActionAuditPayloadForm
)
from djag_scheduler.widgets import JSONFieldWidget


class PeriodicTaskAdmin(admin.ModelAdmin):
    """Admin-interface for periodic tasks."""

    form = PeriodicTaskForm
    model = PeriodicTask
    date_hierarchy = 'date_changed'
    list_display = ('task', 'name', 'crontab', 'enabled', 'running',
                    'last_cron', 'exception_cron', 'total_run_count')
    list_filter = ('enabled', 'running', 'exception_cron')
    actions = ('enable_tasks', 'disable_tasks', 'toggle_tasks', 'clear_exceptions')
    search_fields = ('name', 'task')
    fieldsets = (
        (None, {
            'fields': ('name', 'task', 'enabled', 'skip_misfire', 'coalesce_misfire',
                       'grace_period', 'description',),
            'classes': ('extrapretty', 'wide'),
        }),
        ('Schedule', {
            'fields': ('crontab', 'cron_base', 'running', 'last_cron', 'last_cron_start',
                       'last_cron_end', 'total_run_count', 'exception_cron'),
            'classes': ('extrapretty', 'wide'),
        }),
        ('Arguments', {
            'fields': ('args', 'kwargs'),
            'classes': ('extrapretty', 'wide', 'collapse', 'in'),
        }),
        ('Execution Options', {
            'fields': ('queue', 'exchange',
                       'routing_key', 'priority', 'headers'),
            'classes': ('extrapretty', 'wide', 'collapse', 'in'),
        }),
    )
    readonly_fields = (
        'running', 'last_cron', 'last_cron_start', 'last_cron_end', 'total_run_count'
    )
    formfield_overrides = {
        models.JSONField: {'widget': JSONFieldWidget}
    }

    def _message_user_about_update(self, request, rows_updated, verb):
        """Send message about action to user.
        `verb` should shortly describe what have changed (e.g. 'enabled').
        """
        self.message_user(
            request,
            '{0} task{1} {2} successfully {3}'.format(
                rows_updated,
                pluralize(rows_updated),
                pluralize(rows_updated, 'was,were'),
                verb,
            )
        )

    @classmethod
    def _task_manager(cls, queryset, mode):
        """Operate on tasks based on mode"""
        tasks_updated = 0
        for task in queryset:
            if mode == 'enable':
                task.enabled = True
            elif mode == 'disable':
                task.enabled = False
            elif mode == 'toggle':
                task.enabled = not task.enabled
            elif mode == 'clear':
                task.exception_cron = None

            try:
                if mode == 'clear':
                    task.save(update_fields=['exception_cron'])
                else:
                    task.save(update_fields=['enabled'])
                tasks_updated += 1
            except: # noqa
                pass

        return tasks_updated

    def enable_tasks(self, request, queryset):
        """Enable selected tasks"""
        tasks_updated = PeriodicTaskAdmin._task_manager(queryset, 'enable')
        self._message_user_about_update(request, tasks_updated, 'enabled')

    enable_tasks.short_description = 'Enable selected tasks'

    def disable_tasks(self, request, queryset):
        """Disable selected tasks"""
        tasks_updated = PeriodicTaskAdmin._task_manager(queryset, 'disable')
        self._message_user_about_update(request, tasks_updated, 'disabled')

    disable_tasks.short_description = 'Disable selected tasks'

    def toggle_tasks(self, request, queryset):
        """Toggle selected tasks"""
        tasks_updated = PeriodicTaskAdmin._task_manager(queryset, 'toggle')
        self._message_user_about_update(request, tasks_updated, 'toggled')

    toggle_tasks.short_description = 'Toggle activity of selected tasks'

    def clear_exceptions(self, request, queryset):
        """Clear exceptions for selected tasks"""
        tasks_updated = PeriodicTaskAdmin._task_manager(queryset, 'clear')
        self._message_user_about_update(request, tasks_updated, 'cleared')

    clear_exceptions.short_description = 'Clear exceptions of selected tasks'


class TaskDependencyAdmin(admin.ModelAdmin):
    """Admin interface for Task-Dependency"""

    model = TaskDependency
    list_display = ('__str__', 'depender', 'dependee', 'future_depends', 'change_dt')
    list_filter = ('depender', 'dependee', 'future_depends')
    search_fields = ('depender__name', 'depender__task', 'dependee__name', 'dependee__task')
    readonly_fields = ('change_dt',)

    def get_deleted_objects(self, objs, request):
        """Include related future dependencies"""

        objs_plus_futures = set(objs)
        for obj in objs:
            if isinstance(obj, self.model) and not obj.future_depends:
                if td := obj.__class__.objects.filter(
                    depender=obj.dependee, dependee=obj.depender
                ):
                    objs_plus_futures.add(td[0])

        return super().get_deleted_objects(list(objs_plus_futures), request)


class UserActionAdmin(admin.ModelAdmin):
    """User-Action Admin"""
    list_display = ('action', 'payload', 'create_dt')
    list_filter = ('action',)
    search_fields = ('action', 'payload', 'create_dt')

    def get_form(self, request, obj=None, **kwargs):
        """Use different form for different user-actions"""

        # When editing
        if obj is not None:
            defaults = {'form': get_action_form(obj.action)}
            defaults.update(kwargs)

            return super().get_form(request, obj, **defaults)

        # When adding
        try:
            defaults = {'form': get_action_form(int(request.GET['action']))}
        except (KeyError, TypeError, ValueError):
            raise SuspiciousOperation('action parameter is absent/malformed')

        defaults.update(kwargs)

        action_form = super().get_form(request, obj, **defaults)
        action_form.base_fields['action'].initial = int(request.GET['action'])

        return action_form

    def get_urls(self):
        urls = super().get_urls()
        extra_urls = [
            path(
                'add_payload/',
                self.admin_site.admin_view(self.add_payload_view),
                name='add_action_payload'
            )
        ]

        return extra_urls + urls

    def add_view(self, request, form_url='', extra_context=None):
        """View for selecting user-action: User-action is not saved to db"""
        context = dict(
            self.admin_site.each_context(request),
            action_form=UserActionSelectForm
        )

        return TemplateResponse(request, 'user_action/select_action.html', context)

    def add_payload_view(self, request):
        """View based on selected action: Save user-action with payload to db"""
        return super().add_view(request)

    def get_search_results(self, request, queryset, search_term):
        """Get search results and include results for action choice string"""   # noqa

        queryset, use_distinct = super().get_search_results(request, queryset, search_term)

        search = search_term.strip().lower()
        if search:
            for action_id, action in action_choices.ACTION_CHOICES:
                if search in action.lower():
                    queryset |= UserAction.objects.filter(action=action_id)

        return queryset, use_distinct


class UserActionAuditAdmin(admin.ModelAdmin):
    """User-Action Audit Admin"""

    model = UserActionAudit
    form = UserActionAuditPayloadForm
    list_display = ('action', 'payload', 'create_dt', 'delete_dt')
    list_filter = ('action',)
    search_fields = ('action', 'payload', 'create_dt', 'delete_dt')
    readonly_fields = ('action', 'create_dt', 'delete_dt')
    fieldsets = (
        (None, {
            'fields': ('action', 'payload', 'create_dt', 'delete_dt',),
            'classes': ('extrapretty', 'wide'),
        }),
    )

    def get_search_results(self, request, queryset, search_term):
        """Get search results and include results for action choice string""" # noqa

        queryset, use_distinct = super().get_search_results(request, queryset, search_term)

        search = search_term.strip().lower()
        if search:
            for action_id, action in action_choices.ACTION_CHOICES:
                if search in action.lower():
                    queryset |= UserAction.objects.filter(action=action_id)

        return queryset, use_distinct


admin.site.register(CrontabSchedule)
admin.site.register(TaskDependency, TaskDependencyAdmin)
admin.site.register(PeriodicTask, PeriodicTaskAdmin)
admin.site.register(UserAction, UserActionAdmin)
admin.site.register(UserActionAudit, UserActionAuditAdmin)
