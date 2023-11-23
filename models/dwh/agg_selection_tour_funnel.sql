{% set events = [
    'TourCreationStarted',
    'TourContentAdded',
    'TourSubmitted',
    'TourApproved',
    'FirstBooking',
    'ThirdReview'
    ] %}

select

    tour_id,
    {% for item in ['TourCreationStarted', 'TourContentAdded', 'TourSubmitted', 'TourApproved', ] %}

        min(case when funnel_date)

    {% endfor  %}

