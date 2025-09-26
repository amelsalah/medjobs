from django.db import models

class Job(models.Model):
    title = models.CharField(max_length=255)
    location = models.CharField(max_length=255)
    hospital_name = models.CharField(max_length=255)
    posted_date = models.DateField(null=True, blank=True)
    job_url = models.URLField(null=True, blank=True)
    external_id = models.CharField(max_length=50,null=True, blank=True)  # Oracle HCM Requisition ID

    def __str__(self):
        return self.title
