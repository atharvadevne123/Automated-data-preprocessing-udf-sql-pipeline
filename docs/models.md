# Models Reference

The `models/` package provides Pydantic models for Yelp Open Dataset record types.

## YelpReview

```python
from models.yelp import YelpReview

review = YelpReview(
    review_id="abc123",
    user_id="user456",
    business_id="biz789",
    stars=4.5,
    useful=3,
    funny=1,
    cool=2,
    text="Great experience!",
    date="2023-01-15",
)

review.is_positive()   # True (stars >= 4)
review.is_negative()   # False
review.total_votes()   # 6
```

## YelpBusiness

```python
from models.yelp import YelpBusiness

biz = YelpBusiness(
    business_id="biz789",
    name="Tasty Bistro",
    city="Las Vegas",
    state="NV",
    stars=4.2,
    review_count=342,
    categories="Restaurants, Italian, Pizza",
)

biz.category_list()      # ["Restaurants", "Italian", "Pizza"]
biz.is_highly_rated()    # True (stars >= 4.0)
```

## YelpUser

```python
from models.yelp import YelpUser

user = YelpUser(
    user_id="user456",
    name="Jane Doe",
    review_count=150,
    average_stars=4.3,
    fans=42,
)

user.is_elite_candidate()  # True (review_count >= 100 and average_stars >= 4.0)
```

## Validation

All models validate field constraints at construction time using Pydantic:

- `stars` must be in `[1.0, 5.0]`
- `review_count`, `useful`, `funny`, `cool`, `fans` must be `>= 0`
- `review_id`, `user_id`, `business_id`, `name` must be non-empty strings
