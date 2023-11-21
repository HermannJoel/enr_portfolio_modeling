SELECT SUM(a."P50")
FROM dwh."DimAsset" a 
WHERE  a."InPlanif"  = FALSE
group by a."Technology";