-- ============================================================
-- UC-16: Customer Satisfaction & NPS Tracking
-- Domain: Customer Experience / CRM
-- Business Question: How satisfied are our customers?
--   Which product types generate negative feedback?
--   What are Finnish vs international customers saying?
-- Priority: P2 | Users: Sales, Customer Service, Marketing
-- ============================================================

WITH feedback_enriched AS (
  SELECT
    f.feedback_id,
    f.customer_name,
    f.feedback_date,
    DATE_TRUNC(f.feedback_date, MONTH)     AS feedback_month,
    f.channel,
    f.rating,
    f.feedback_text,
    f.language,
    f.product_type,
    dc.country,
    dc.region,
    dc.customer_type,
    -- NPS categorisation (on 1-5 scale mapped to NPS logic)
    CASE
      WHEN f.rating >= 5  THEN 'PROMOTER'
      WHEN f.rating = 3   THEN 'PASSIVE'
      WHEN f.rating <= 2  THEN 'DETRACTOR'
      ELSE 'PASSIVE'
    END AS nps_category,
    -- Sentiment keywords (basic rule-based, Vertex AI replaces in Chapter 07)
    CASE
      WHEN LOWER(f.feedback_text) LIKE '%excellent%'
        OR LOWER(f.feedback_text) LIKE '%perfect%'
        OR LOWER(f.feedback_text) LIKE '%erinomainen%'  -- Finnish "excellent"
        OR LOWER(f.feedback_text) LIKE '%paras%'         -- Finnish "best"
        THEN 'POSITIVE'
      WHEN LOWER(f.feedback_text) LIKE '%poor%'
        OR LOWER(f.feedback_text) LIKE '%defect%'
        OR LOWER(f.feedback_text) LIKE '%unacceptable%'
        OR LOWER(f.feedback_text) LIKE '%below%'
        THEN 'NEGATIVE'
      ELSE 'NEUTRAL'
    END AS rule_based_sentiment
  FROM `nordleaf_stg.customer_feedback` f
  LEFT JOIN `nordleaf_dwh.dim_customer` dc
    ON f.customer_name = dc.customer_name AND dc.is_current
)

SELECT
  feedback_month,
  product_type,
  language,
  region,
  COUNT(*)                                      AS feedback_count,
  ROUND(AVG(rating), 2)                         AS avg_rating,
  COUNTIF(nps_category = 'PROMOTER')            AS promoters,
  COUNTIF(nps_category = 'PASSIVE')             AS passives,
  COUNTIF(nps_category = 'DETRACTOR')           AS detractors,
  -- NPS = % Promoters - % Detractors (scale -100 to +100)
  ROUND(
    (COUNTIF(nps_category='PROMOTER') - COUNTIF(nps_category='DETRACTOR'))
    / NULLIF(COUNT(*), 0) * 100
  , 0)                                           AS nps_score,
  COUNTIF(rule_based_sentiment = 'POSITIVE')    AS positive_sentiment,
  COUNTIF(rule_based_sentiment = 'NEGATIVE')    AS negative_sentiment,
  -- Alert threshold
  CASE
    WHEN AVG(rating) < 3.0 THEN 'CRITICAL — escalate to account manager'
    WHEN AVG(rating) < 3.5 THEN 'WARNING — schedule customer call'
    ELSE 'HEALTHY'
  END AS satisfaction_status
FROM feedback_enriched
GROUP BY 1,2,3,4
ORDER BY 1 DESC, avg_rating ASC;
