"""Task Dependency model"""

from collections import defaultdict

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import signals
from toposort import CircularDependencyError
from toposort import toposort

import djag_scheduler.models.user_action_model as action_choices
from .periodic_task_model import PeriodicTask
from .user_action_model import UserAction


class TaskDependency(models.Model):
    """Model representing Djag-Scheduler task dependencies"""

    depender = models.ForeignKey(
        PeriodicTask, on_delete=models.CASCADE, related_name='depender_task',
        verbose_name='Depender', help_text='Task Dependent on Dependee'
    )
    dependee = models.ForeignKey(
        PeriodicTask, on_delete=models.CASCADE, related_name='dependee_task',
        verbose_name='Dependee', help_text='Task Dependent by Depender'
    )
    future_depends = models.BooleanField(
        default=False, verbose_name='Future Dependency',
        help_text='Depender is Future Dependent on Dependee'
    )
    change_dt = models.DateTimeField(
        auto_now=True, verbose_name='Change Date',
        help_text='Date Time at which dependency is created/changed'
    )

    class Meta:
        """Table Information"""

        verbose_name = 'Task Dependency'
        verbose_name_plural = 'Task Dependencies'
        constraints = [
            models.UniqueConstraint(
                name='task_set',
                fields=['depender', 'dependee']
            )
        ]

    def clean(self):
        """Clean model data"""

        # If the <depender, dependee> already exists let the unique key validator
        # throw the error
        if td := self.__class__.objects.filter(
                depender=self.depender, dependee=self.dependee
        ):
            # In case of update self.pk == td[0].pk (needs further validation)
            if self.pk != td[0].pk:
                return

        # Validate self dependency
        if self.depender == self.dependee and not self.future_depends:
            raise ValidationError('A task can only future depend on itself')

        # Validate dependency graph constraints and basic cycle-detection
        direct_deps = set()
        future_deps = set()
        task_groups = defaultdict(set)  # Group tasks by depender

        tds = self.__class__.objects.all()
        if self._state.adding:
            tds = *tds, self

        for td in tds:
            if td.pk == self.pk:
                # In case of update accept new changes
                td = self

            if td.future_depends:
                future_deps.add((td.dependee, td.depender))

                if td.depender == td.dependee:
                    # Add self dependency to direct deps as well
                    direct_deps.add((td.depender, td.dependee))
            else:
                direct_deps.add((td.depender, td.dependee))

                # Add only direct dependencies to task group
                task_groups[td.depender.id].add(td.dependee.id)

        # Validate dependency graph constraints
        orphans = future_deps - direct_deps
        if orphans:
            raise ValidationError('This change will orphan {0}'.format(
                ', '.join(['{0} --D+ {1}'.format(orphan[1].name, orphan[0].name) for orphan in orphans])
            ))

        # Detect basic cycles
        try:
            _ = tuple(toposort(task_groups))
        except CircularDependencyError:
            raise ValidationError('Task-Dependency creates a cycle in DAG')

    def save(self, *args, **kwargs):
        """Save model data"""
        self.full_clean()

        # Call super().save() only when there are real changes
        if not self.pk:
            # Object created for the first time
            super().save(*args, **kwargs)
        else:
            cls = self.__class__
            old = cls.objects.get(pk=self.pk)

            # Check for changes (use update_fields if set)
            for field in (kwargs.get('update_fields') or [field.name for field in cls._meta.get_fields()]):
                try:
                    if getattr(old, field) != getattr(self, field):
                        try:
                            super().save(*args, **kwargs)
                        except Exception as exc:
                            raise exc
                        break
                except:  # noqa
                    raise ValueError('Failed to compare the field ' + field + ' between the current and old model')

    def __str__(self):
        return '{0} --{1} {2}'.format(
            self.depender.name,
            "D+" if self.future_depends else "D",
            self.dependee.name
        )

    @classmethod
    def insert_dependency_change(cls, instance, *args, **kwargs):
        """Insert Task Deleted record into UserAction"""
        if not isinstance(instance, TaskDependency):
            return

        user_action = UserAction(
            action=action_choices.DEPENDENCY_CHANGED,
            payload=dict(
                schedule_id=instance.id,
                schedule=str(instance)
            )
        )
        user_action.save()


signals.pre_save.connect(TaskDependency.insert_dependency_change, sender=TaskDependency)
signals.pre_delete.connect(TaskDependency.insert_dependency_change, sender=TaskDependency)
