SELECT SUM(a."P50")
FROM stagging."Asset" a 
WHERE  a."InPlanif" = false
group by a."Technology";