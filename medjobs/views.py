from django.shortcuts import render
from django.db.models import Q, Count
from django.core.paginator import Paginator
from .models import Job

def job_list(request):
    query = request.GET.get('q')
    hospital = request.GET.get('hospital')
    city = request.GET.get('city')
    page = request.GET.get('page', 1)

    # --- base queryset for filtering ---
    jobs_qs = Job.objects.all()

    if query:
        jobs_qs = jobs_qs.filter(
            Q(title__icontains=query) |
            Q(location__icontains=query) |
            Q(hospital_name__icontains=query)
        )

    if hospital:
        jobs_qs = jobs_qs.filter(hospital_name__icontains=hospital)

    if city:
        jobs_qs = jobs_qs.filter(location__icontains=city)

    # --- pagination (10 per page) ---
    paginator = Paginator(jobs_qs.order_by("-posted_date"), 10)
    jobs = paginator.get_page(page)

    # --- global counts (always from full table, not filtered) ---
    hospital_counts = Job.objects.values("hospital_name").annotate(total=Count("id")).order_by("hospital_name")
    city_counts = Job.objects.values("location").annotate(total=Count("id")).order_by("location")
    total_jobs = Job.objects.count()

    return render(request, "medjobs/job_list.html", {
        "jobs": jobs,
        "query": query,
        "hospital_counts": hospital_counts,
        "city_counts": city_counts,
        "total_jobs": total_jobs,
        "hospital": hospital,
        "city": city,
    })
