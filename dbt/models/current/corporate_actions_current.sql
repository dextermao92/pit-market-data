-- Latest version of each action as of now. Point-in-time versions are applied at query time (pit.adjust).
select *
from {{ source('staged', 'corporate_actions') }}
qualify row_number() over (partition by id order by ingested_at desc) = 1
