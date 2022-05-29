{{ config(
    materialized = 'table'
) }}
--

with date_spine as
(
    
        select '2022-04-01 00:00:00' as date_day union all 
    
        select '2022-04-02 00:00:00' as date_day union all 
    
        select '2022-04-03 00:00:00' as date_day union all 
    
        select '2022-04-04 00:00:00' as date_day union all 
    
        select '2022-04-05 00:00:00' as date_day union all 
    
        select '2022-04-06 00:00:00' as date_day union all 
    
        select '2022-04-07 00:00:00' as date_day union all 
    
        select '2022-04-08 00:00:00' as date_day union all 
    
        select '2022-04-09 00:00:00' as date_day union all 
    
        select '2022-04-10 00:00:00' as date_day union all 
    
        select '2022-04-11 00:00:00' as date_day union all 
    
        select '2022-04-12 00:00:00' as date_day union all 
    
        select '2022-04-13 00:00:00' as date_day union all 
    
        select '2022-04-14 00:00:00' as date_day union all 
    
        select '2022-04-15 00:00:00' as date_day union all 
    
        select '2022-04-16 00:00:00' as date_day union all 
    
        select '2022-04-17 00:00:00' as date_day union all 
    
        select '2022-04-18 00:00:00' as date_day union all 
    
        select '2022-04-19 00:00:00' as date_day union all 
    
        select '2022-04-20 00:00:00' as date_day union all 
    
        select '2022-04-21 00:00:00' as date_day union all 
    
        select '2022-04-22 00:00:00' as date_day union all 
    
        select '2022-04-23 00:00:00' as date_day union all 
    
        select '2022-04-24 00:00:00' as date_day union all 
    
        select '2022-04-25 00:00:00' as date_day union all 
    
        select '2022-04-26 00:00:00' as date_day union all 
    
        select '2022-04-27 00:00:00' as date_day union all 
    
        select '2022-04-28 00:00:00' as date_day union all 
    
        select '2022-04-29 00:00:00' as date_day union all 
    
        select '2022-04-30 00:00:00' as date_day union all 
    
        select '2022-05-01 00:00:00' as date_day union all 
    
        select '2022-05-02 00:00:00' as date_day union all 
    
        select '2022-05-03 00:00:00' as date_day union all 
    
        select '2022-05-04 00:00:00' as date_day union all 
    
        select '2022-05-05 00:00:00' as date_day union all 
    
        select '2022-05-06 00:00:00' as date_day union all 
    
        select '2022-05-07 00:00:00' as date_day union all 
    
        select '2022-05-08 00:00:00' as date_day union all 
    
        select '2022-05-09 00:00:00' as date_day union all 
    
        select '2022-05-10 00:00:00' as date_day union all 
    
        select '2022-05-11 00:00:00' as date_day union all 
    
        select '2022-05-12 00:00:00' as date_day union all 
    
        select '2022-05-13 00:00:00' as date_day union all 
    
        select '2022-05-14 00:00:00' as date_day union all 
    
        select '2022-05-15 00:00:00' as date_day union all 
    
        select '2022-05-16 00:00:00' as date_day union all 
    
        select '2022-05-17 00:00:00' as date_day union all 
    
        select '2022-05-18 00:00:00' as date_day union all 
    
        select '2022-05-19 00:00:00' as date_day union all 
    
        select '2022-05-20 00:00:00' as date_day union all 
    
        select '2022-05-21 00:00:00' as date_day union all 
    
        select '2022-05-22 00:00:00' as date_day union all 
    
        select '2022-05-23 00:00:00' as date_day union all 
    
        select '2022-05-24 00:00:00' as date_day union all 
    
        select '2022-05-25 00:00:00' as date_day union all 
    
        select '2022-05-26 00:00:00' as date_day union all 
    
        select '2022-05-27 00:00:00' as date_day union all 
    
        select '2022-05-28 00:00:00' as date_day union all 
    
        select '2022-05-29 00:00:00' as date_day union all 
    
        select '2022-05-30 00:00:00' as date_day union all 
    
        select '2022-05-31 00:00:00' as date_day union all 
    
        select '2022-06-01 00:00:00' as date_day union all 
    
        select '2022-06-02 00:00:00' as date_day union all 
    
        select '2022-06-03 00:00:00' as date_day union all 
    
        select '2022-06-04 00:00:00' as date_day union all 
    
        select '2022-06-05 00:00:00' as date_day union all 
    
        select '2022-06-06 00:00:00' as date_day union all 
    
        select '2022-06-07 00:00:00' as date_day union all 
    
        select '2022-06-08 00:00:00' as date_day union all 
    
        select '2022-06-09 00:00:00' as date_day union all 
    
        select '2022-06-10 00:00:00' as date_day union all 
    
        select '2022-06-11 00:00:00' as date_day union all 
    
        select '2022-06-12 00:00:00' as date_day union all 
    
        select '2022-06-13 00:00:00' as date_day union all 
    
        select '2022-06-14 00:00:00' as date_day union all 
    
        select '2022-06-15 00:00:00' as date_day union all 
    
        select '2022-06-16 00:00:00' as date_day union all 
    
        select '2022-06-17 00:00:00' as date_day union all 
    
        select '2022-06-18 00:00:00' as date_day union all 
    
        select '2022-06-19 00:00:00' as date_day union all 
    
        select '2022-06-20 00:00:00' as date_day union all 
    
        select '2022-06-21 00:00:00' as date_day union all 
    
        select '2022-06-22 00:00:00' as date_day union all 
    
        select '2022-06-23 00:00:00' as date_day union all 
    
        select '2022-06-24 00:00:00' as date_day union all 
    
        select '2022-06-25 00:00:00' as date_day union all 
    
        select '2022-06-26 00:00:00' as date_day union all 
    
        select '2022-06-27 00:00:00' as date_day union all 
    
        select '2022-06-28 00:00:00' as date_day union all 
    
        select '2022-06-29 00:00:00' as date_day union all 
    
        select '2022-06-30 00:00:00' as date_day union all 
    
        select '2022-07-01 00:00:00' as date_day union all 
    
        select '2022-07-02 00:00:00' as date_day union all 
    
        select '2022-07-03 00:00:00' as date_day union all 
    
        select '2022-07-04 00:00:00' as date_day union all 
    
        select '2022-07-05 00:00:00' as date_day union all 
    
        select '2022-07-06 00:00:00' as date_day union all 
    
        select '2022-07-07 00:00:00' as date_day union all 
    
        select '2022-07-08 00:00:00' as date_day union all 
    
        select '2022-07-09 00:00:00' as date_day union all 
    
        select '2022-07-10 00:00:00' as date_day union all 
    
        select '2022-07-11 00:00:00' as date_day union all 
    
        select '2022-07-12 00:00:00' as date_day union all 
    
        select '2022-07-13 00:00:00' as date_day union all 
    
        select '2022-07-14 00:00:00' as date_day union all 
    
        select '2022-07-15 00:00:00' as date_day union all 
    
        select '2022-07-16 00:00:00' as date_day union all 
    
        select '2022-07-17 00:00:00' as date_day union all 
    
        select '2022-07-18 00:00:00' as date_day union all 
    
        select '2022-07-19 00:00:00' as date_day union all 
    
        select '2022-07-20 00:00:00' as date_day union all 
    
        select '2022-07-21 00:00:00' as date_day union all 
    
        select '2022-07-22 00:00:00' as date_day union all 
    
        select '2022-07-23 00:00:00' as date_day union all 
    
        select '2022-07-24 00:00:00' as date_day union all 
    
        select '2022-07-25 00:00:00' as date_day union all 
    
        select '2022-07-26 00:00:00' as date_day union all 
    
        select '2022-07-27 00:00:00' as date_day union all 
    
        select '2022-07-28 00:00:00' as date_day union all 
    
        select '2022-07-29 00:00:00' as date_day union all 
    
        select '2022-07-30 00:00:00' as date_day union all 
    
        select '2022-07-31 00:00:00' as date_day union all 
    
        select '2022-08-01 00:00:00' as date_day union all 
    
        select '2022-08-02 00:00:00' as date_day union all 
    
        select '2022-08-03 00:00:00' as date_day union all 
    
        select '2022-08-04 00:00:00' as date_day union all 
    
        select '2022-08-05 00:00:00' as date_day union all 
    
        select '2022-08-06 00:00:00' as date_day union all 
    
        select '2022-08-07 00:00:00' as date_day union all 
    
        select '2022-08-08 00:00:00' as date_day union all 
    
        select '2022-08-09 00:00:00' as date_day union all 
    
        select '2022-08-10 00:00:00' as date_day union all 
    
        select '2022-08-11 00:00:00' as date_day union all 
    
        select '2022-08-12 00:00:00' as date_day union all 
    
        select '2022-08-13 00:00:00' as date_day union all 
    
        select '2022-08-14 00:00:00' as date_day union all 
    
        select '2022-08-15 00:00:00' as date_day union all 
    
        select '2022-08-16 00:00:00' as date_day union all 
    
        select '2022-08-17 00:00:00' as date_day union all 
    
        select '2022-08-18 00:00:00' as date_day union all 
    
        select '2022-08-19 00:00:00' as date_day union all 
    
        select '2022-08-20 00:00:00' as date_day union all 
    
        select '2022-08-21 00:00:00' as date_day union all 
    
        select '2022-08-22 00:00:00' as date_day union all 
    
        select '2022-08-23 00:00:00' as date_day union all 
    
        select '2022-08-24 00:00:00' as date_day union all 
    
        select '2022-08-25 00:00:00' as date_day union all 
    
        select '2022-08-26 00:00:00' as date_day union all 
    
        select '2022-08-27 00:00:00' as date_day union all 
    
        select '2022-08-28 00:00:00' as date_day union all 
    
        select '2022-08-29 00:00:00' as date_day union all 
    
        select '2022-08-30 00:00:00' as date_day union all 
    
        select '2022-08-31 00:00:00' as date_day union all 
    
        select '2022-09-01 00:00:00' as date_day union all 
    
        select '2022-09-02 00:00:00' as date_day union all 
    
        select '2022-09-03 00:00:00' as date_day union all 
    
        select '2022-09-04 00:00:00' as date_day union all 
    
        select '2022-09-05 00:00:00' as date_day union all 
    
        select '2022-09-06 00:00:00' as date_day union all 
    
        select '2022-09-07 00:00:00' as date_day union all 
    
        select '2022-09-08 00:00:00' as date_day union all 
    
        select '2022-09-09 00:00:00' as date_day union all 
    
        select '2022-09-10 00:00:00' as date_day union all 
    
        select '2022-09-11 00:00:00' as date_day union all 
    
        select '2022-09-12 00:00:00' as date_day union all 
    
        select '2022-09-13 00:00:00' as date_day union all 
    
        select '2022-09-14 00:00:00' as date_day union all 
    
        select '2022-09-15 00:00:00' as date_day union all 
    
        select '2022-09-16 00:00:00' as date_day union all 
    
        select '2022-09-17 00:00:00' as date_day union all 
    
        select '2022-09-18 00:00:00' as date_day union all 
    
        select '2022-09-19 00:00:00' as date_day union all 
    
        select '2022-09-20 00:00:00' as date_day union all 
    
        select '2022-09-21 00:00:00' as date_day union all 
    
        select '2022-09-22 00:00:00' as date_day union all 
    
        select '2022-09-23 00:00:00' as date_day union all 
    
        select '2022-09-24 00:00:00' as date_day union all 
    
        select '2022-09-25 00:00:00' as date_day union all 
    
        select '2022-09-26 00:00:00' as date_day union all 
    
        select '2022-09-27 00:00:00' as date_day union all 
    
        select '2022-09-28 00:00:00' as date_day union all 
    
        select '2022-09-29 00:00:00' as date_day union all 
    
        select '2022-09-30 00:00:00' as date_day union all 
    
        select '2022-10-01 00:00:00' as date_day union all 
    
        select '2022-10-02 00:00:00' as date_day union all 
    
        select '2022-10-03 00:00:00' as date_day union all 
    
        select '2022-10-04 00:00:00' as date_day union all 
    
        select '2022-10-05 00:00:00' as date_day union all 
    
        select '2022-10-06 00:00:00' as date_day union all 
    
        select '2022-10-07 00:00:00' as date_day union all 
    
        select '2022-10-08 00:00:00' as date_day union all 
    
        select '2022-10-09 00:00:00' as date_day union all 
    
        select '2022-10-10 00:00:00' as date_day union all 
    
        select '2022-10-11 00:00:00' as date_day union all 
    
        select '2022-10-12 00:00:00' as date_day union all 
    
        select '2022-10-13 00:00:00' as date_day union all 
    
        select '2022-10-14 00:00:00' as date_day union all 
    
        select '2022-10-15 00:00:00' as date_day union all 
    
        select '2022-10-16 00:00:00' as date_day union all 
    
        select '2022-10-17 00:00:00' as date_day union all 
    
        select '2022-10-18 00:00:00' as date_day union all 
    
        select '2022-10-19 00:00:00' as date_day union all 
    
        select '2022-10-20 00:00:00' as date_day union all 
    
        select '2022-10-21 00:00:00' as date_day union all 
    
        select '2022-10-22 00:00:00' as date_day union all 
    
        select '2022-10-23 00:00:00' as date_day union all 
    
        select '2022-10-24 00:00:00' as date_day union all 
    
        select '2022-10-25 00:00:00' as date_day union all 
    
        select '2022-10-26 00:00:00' as date_day union all 
    
        select '2022-10-27 00:00:00' as date_day union all 
    
        select '2022-10-28 00:00:00' as date_day union all 
    
        select '2022-10-29 00:00:00' as date_day union all 
    
        select '2022-10-30 00:00:00' as date_day union all 
    
        select '2022-10-31 00:00:00' as date_day union all 
    
        select '2022-11-01 00:00:00' as date_day union all 
    
        select '2022-11-02 00:00:00' as date_day union all 
    
        select '2022-11-03 00:00:00' as date_day union all 
    
        select '2022-11-04 00:00:00' as date_day union all 
    
        select '2022-11-05 00:00:00' as date_day union all 
    
        select '2022-11-06 00:00:00' as date_day union all 
    
        select '2022-11-07 00:00:00' as date_day union all 
    
        select '2022-11-08 00:00:00' as date_day union all 
    
        select '2022-11-09 00:00:00' as date_day union all 
    
        select '2022-11-10 00:00:00' as date_day union all 
    
        select '2022-11-11 00:00:00' as date_day union all 
    
        select '2022-11-12 00:00:00' as date_day union all 
    
        select '2022-11-13 00:00:00' as date_day union all 
    
        select '2022-11-14 00:00:00' as date_day union all 
    
        select '2022-11-15 00:00:00' as date_day union all 
    
        select '2022-11-16 00:00:00' as date_day union all 
    
        select '2022-11-17 00:00:00' as date_day union all 
    
        select '2022-11-18 00:00:00' as date_day union all 
    
        select '2022-11-19 00:00:00' as date_day union all 
    
        select '2022-11-20 00:00:00' as date_day union all 
    
        select '2022-11-21 00:00:00' as date_day union all 
    
        select '2022-11-22 00:00:00' as date_day union all 
    
        select '2022-11-23 00:00:00' as date_day union all 
    
        select '2022-11-24 00:00:00' as date_day union all 
    
        select '2022-11-25 00:00:00' as date_day union all 
    
        select '2022-11-26 00:00:00' as date_day union all 
    
        select '2022-11-27 00:00:00' as date_day union all 
    
        select '2022-11-28 00:00:00' as date_day union all 
    
        select '2022-11-29 00:00:00' as date_day union all 
    
        select '2022-11-30 00:00:00' as date_day union all 
    
        select '2022-12-01 00:00:00' as date_day union all 
    
        select '2022-12-02 00:00:00' as date_day union all 
    
        select '2022-12-03 00:00:00' as date_day union all 
    
        select '2022-12-04 00:00:00' as date_day union all 
    
        select '2022-12-05 00:00:00' as date_day union all 
    
        select '2022-12-06 00:00:00' as date_day union all 
    
        select '2022-12-07 00:00:00' as date_day union all 
    
        select '2022-12-08 00:00:00' as date_day union all 
    
        select '2022-12-09 00:00:00' as date_day union all 
    
        select '2022-12-10 00:00:00' as date_day union all 
    
        select '2022-12-11 00:00:00' as date_day union all 
    
        select '2022-12-12 00:00:00' as date_day union all 
    
        select '2022-12-13 00:00:00' as date_day union all 
    
        select '2022-12-14 00:00:00' as date_day union all 
    
        select '2022-12-15 00:00:00' as date_day union all 
    
        select '2022-12-16 00:00:00' as date_day union all 
    
        select '2022-12-17 00:00:00' as date_day union all 
    
        select '2022-12-18 00:00:00' as date_day union all 
    
        select '2022-12-19 00:00:00' as date_day union all 
    
        select '2022-12-20 00:00:00' as date_day union all 
    
        select '2022-12-21 00:00:00' as date_day union all 
    
        select '2022-12-22 00:00:00' as date_day union all 
    
        select '2022-12-23 00:00:00' as date_day union all 
    
        select '2022-12-24 00:00:00' as date_day union all 
    
        select '2022-12-25 00:00:00' as date_day union all 
    
        select '2022-12-26 00:00:00' as date_day union all 
    
        select '2022-12-27 00:00:00' as date_day union all 
    
        select '2022-12-28 00:00:00' as date_day union all 
    
        select '2022-12-29 00:00:00' as date_day union all 
    
        select '2022-12-30 00:00:00' as date_day union all 
    
        select '2022-12-31 00:00:00' as date_day union all 
    
        select '2023-01-01 00:00:00' as date_day union all 
    
        select '2023-01-02 00:00:00' as date_day union all 
    
        select '2023-01-03 00:00:00' as date_day union all 
    
        select '2023-01-04 00:00:00' as date_day union all 
    
        select '2023-01-05 00:00:00' as date_day union all 
    
        select '2023-01-06 00:00:00' as date_day union all 
    
        select '2023-01-07 00:00:00' as date_day union all 
    
        select '2023-01-08 00:00:00' as date_day union all 
    
        select '2023-01-09 00:00:00' as date_day union all 
    
        select '2023-01-10 00:00:00' as date_day union all 
    
        select '2023-01-11 00:00:00' as date_day union all 
    
        select '2023-01-12 00:00:00' as date_day union all 
    
        select '2023-01-13 00:00:00' as date_day union all 
    
        select '2023-01-14 00:00:00' as date_day union all 
    
        select '2023-01-15 00:00:00' as date_day union all 
    
        select '2023-01-16 00:00:00' as date_day union all 
    
        select '2023-01-17 00:00:00' as date_day union all 
    
        select '2023-01-18 00:00:00' as date_day union all 
    
        select '2023-01-19 00:00:00' as date_day union all 
    
        select '2023-01-20 00:00:00' as date_day union all 
    
        select '2023-01-21 00:00:00' as date_day union all 
    
        select '2023-01-22 00:00:00' as date_day union all 
    
        select '2023-01-23 00:00:00' as date_day union all 
    
        select '2023-01-24 00:00:00' as date_day union all 
    
        select '2023-01-25 00:00:00' as date_day union all 
    
        select '2023-01-26 00:00:00' as date_day union all 
    
        select '2023-01-27 00:00:00' as date_day union all 
    
        select '2023-01-28 00:00:00' as date_day union all 
    
        select '2023-01-29 00:00:00' as date_day union all 
    
        select '2023-01-30 00:00:00' as date_day union all 
    
        select '2023-01-31 00:00:00' as date_day union all 
    
        select '2023-02-01 00:00:00' as date_day union all 
    
        select '2023-02-02 00:00:00' as date_day union all 
    
        select '2023-02-03 00:00:00' as date_day union all 
    
        select '2023-02-04 00:00:00' as date_day union all 
    
        select '2023-02-05 00:00:00' as date_day union all 
    
        select '2023-02-06 00:00:00' as date_day union all 
    
        select '2023-02-07 00:00:00' as date_day union all 
    
        select '2023-02-08 00:00:00' as date_day union all 
    
        select '2023-02-09 00:00:00' as date_day union all 
    
        select '2023-02-10 00:00:00' as date_day union all 
    
        select '2023-02-11 00:00:00' as date_day union all 
    
        select '2023-02-12 00:00:00' as date_day union all 
    
        select '2023-02-13 00:00:00' as date_day union all 
    
        select '2023-02-14 00:00:00' as date_day union all 
    
        select '2023-02-15 00:00:00' as date_day union all 
    
        select '2023-02-16 00:00:00' as date_day union all 
    
        select '2023-02-17 00:00:00' as date_day union all 
    
        select '2023-02-18 00:00:00' as date_day union all 
    
        select '2023-02-19 00:00:00' as date_day union all 
    
        select '2023-02-20 00:00:00' as date_day union all 
    
        select '2023-02-21 00:00:00' as date_day union all 
    
        select '2023-02-22 00:00:00' as date_day union all 
    
        select '2023-02-23 00:00:00' as date_day union all 
    
        select '2023-02-24 00:00:00' as date_day union all 
    
        select '2023-02-25 00:00:00' as date_day union all 
    
        select '2023-02-26 00:00:00' as date_day union all 
    
        select '2023-02-27 00:00:00' as date_day union all 
    
        select '2023-02-28 00:00:00' as date_day union all 
    
        select '2023-03-01 00:00:00' as date_day union all 
    
        select '2023-03-02 00:00:00' as date_day union all 
    
        select '2023-03-03 00:00:00' as date_day union all 
    
        select '2023-03-04 00:00:00' as date_day union all 
    
        select '2023-03-05 00:00:00' as date_day union all 
    
        select '2023-03-06 00:00:00' as date_day union all 
    
        select '2023-03-07 00:00:00' as date_day union all 
    
        select '2023-03-08 00:00:00' as date_day union all 
    
        select '2023-03-09 00:00:00' as date_day union all 
    
        select '2023-03-10 00:00:00' as date_day union all 
    
        select '2023-03-11 00:00:00' as date_day union all 
    
        select '2023-03-12 00:00:00' as date_day union all 
    
        select '2023-03-13 00:00:00' as date_day union all 
    
        select '2023-03-14 00:00:00' as date_day union all 
    
        select '2023-03-15 00:00:00' as date_day union all 
    
        select '2023-03-16 00:00:00' as date_day union all 
    
        select '2023-03-17 00:00:00' as date_day union all 
    
        select '2023-03-18 00:00:00' as date_day union all 
    
        select '2023-03-19 00:00:00' as date_day union all 
    
        select '2023-03-20 00:00:00' as date_day union all 
    
        select '2023-03-21 00:00:00' as date_day union all 
    
        select '2023-03-22 00:00:00' as date_day union all 
    
        select '2023-03-23 00:00:00' as date_day union all 
    
        select '2023-03-24 00:00:00' as date_day union all 
    
        select '2023-03-25 00:00:00' as date_day union all 
    
        select '2023-03-26 00:00:00' as date_day union all 
    
        select '2023-03-27 00:00:00' as date_day union all 
    
        select '2023-03-28 00:00:00' as date_day union all 
    
        select '2023-03-29 00:00:00' as date_day union all 
    
        select '2023-03-30 00:00:00' as date_day union all 
    
        select '2023-03-31 00:00:00' as date_day union all 
    
        select '2023-04-01 00:00:00' as date_day union all 
    
        select '2023-04-02 00:00:00' as date_day union all 
    
        select '2023-04-03 00:00:00' as date_day union all 
    
        select '2023-04-04 00:00:00' as date_day union all 
    
        select '2023-04-05 00:00:00' as date_day union all 
    
        select '2023-04-06 00:00:00' as date_day union all 
    
        select '2023-04-07 00:00:00' as date_day union all 
    
        select '2023-04-08 00:00:00' as date_day union all 
    
        select '2023-04-09 00:00:00' as date_day union all 
    
        select '2023-04-10 00:00:00' as date_day union all 
    
        select '2023-04-11 00:00:00' as date_day union all 
    
        select '2023-04-12 00:00:00' as date_day union all 
    
        select '2023-04-13 00:00:00' as date_day union all 
    
        select '2023-04-14 00:00:00' as date_day union all 
    
        select '2023-04-15 00:00:00' as date_day union all 
    
        select '2023-04-16 00:00:00' as date_day union all 
    
        select '2023-04-17 00:00:00' as date_day union all 
    
        select '2023-04-18 00:00:00' as date_day union all 
    
        select '2023-04-19 00:00:00' as date_day union all 
    
        select '2023-04-20 00:00:00' as date_day union all 
    
        select '2023-04-21 00:00:00' as date_day union all 
    
        select '2023-04-22 00:00:00' as date_day union all 
    
        select '2023-04-23 00:00:00' as date_day union all 
    
        select '2023-04-24 00:00:00' as date_day union all 
    
        select '2023-04-25 00:00:00' as date_day union all 
    
        select '2023-04-26 00:00:00' as date_day union all 
    
        select '2023-04-27 00:00:00' as date_day union all 
    
        select '2023-04-28 00:00:00' as date_day union all 
    
        select '2023-04-29 00:00:00' as date_day union all 
    
        select '2023-04-30 00:00:00' as date_day union all 
    
        select '2023-05-01 00:00:00' as date_day union all 
    
        select '2023-05-02 00:00:00' as date_day union all 
    
        select '2023-05-03 00:00:00' as date_day union all 
    
        select '2023-05-04 00:00:00' as date_day union all 
    
        select '2023-05-05 00:00:00' as date_day union all 
    
        select '2023-05-06 00:00:00' as date_day union all 
    
        select '2023-05-07 00:00:00' as date_day union all 
    
        select '2023-05-08 00:00:00' as date_day union all 
    
        select '2023-05-09 00:00:00' as date_day union all 
    
        select '2023-05-10 00:00:00' as date_day union all 
    
        select '2023-05-11 00:00:00' as date_day union all 
    
        select '2023-05-12 00:00:00' as date_day union all 
    
        select '2023-05-13 00:00:00' as date_day union all 
    
        select '2023-05-14 00:00:00' as date_day union all 
    
        select '2023-05-15 00:00:00' as date_day union all 
    
        select '2023-05-16 00:00:00' as date_day union all 
    
        select '2023-05-17 00:00:00' as date_day union all 
    
        select '2023-05-18 00:00:00' as date_day union all 
    
        select '2023-05-19 00:00:00' as date_day union all 
    
        select '2023-05-20 00:00:00' as date_day union all 
    
        select '2023-05-21 00:00:00' as date_day union all 
    
        select '2023-05-22 00:00:00' as date_day union all 
    
        select '2023-05-23 00:00:00' as date_day union all 
    
        select '2023-05-24 00:00:00' as date_day union all 
    
        select '2023-05-25 00:00:00' as date_day union all 
    
        select '2023-05-26 00:00:00' as date_day union all 
    
        select '2023-05-27 00:00:00' as date_day union all 
    
        select '2023-05-28 00:00:00' as date_day union all 
    
        select '2023-05-29 00:00:00' as date_day union all 
    
        select '2023-05-30 00:00:00' as date_day union all 
    
        select '2023-05-31 00:00:00' as date_day union all 
    
        select '2023-06-01 00:00:00' as date_day union all 
    
        select '2023-06-02 00:00:00' as date_day union all 
    
        select '2023-06-03 00:00:00' as date_day union all 
    
        select '2023-06-04 00:00:00' as date_day union all 
    
        select '2023-06-05 00:00:00' as date_day union all 
    
        select '2023-06-06 00:00:00' as date_day union all 
    
        select '2023-06-07 00:00:00' as date_day union all 
    
        select '2023-06-08 00:00:00' as date_day union all 
    
        select '2023-06-09 00:00:00' as date_day union all 
    
        select '2023-06-10 00:00:00' as date_day union all 
    
        select '2023-06-11 00:00:00' as date_day union all 
    
        select '2023-06-12 00:00:00' as date_day union all 
    
        select '2023-06-13 00:00:00' as date_day union all 
    
        select '2023-06-14 00:00:00' as date_day union all 
    
        select '2023-06-15 00:00:00' as date_day union all 
    
        select '2023-06-16 00:00:00' as date_day union all 
    
        select '2023-06-17 00:00:00' as date_day union all 
    
        select '2023-06-18 00:00:00' as date_day union all 
    
        select '2023-06-19 00:00:00' as date_day union all 
    
        select '2023-06-20 00:00:00' as date_day union all 
    
        select '2023-06-21 00:00:00' as date_day union all 
    
        select '2023-06-22 00:00:00' as date_day union all 
    
        select '2023-06-23 00:00:00' as date_day union all 
    
        select '2023-06-24 00:00:00' as date_day union all 
    
        select '2023-06-25 00:00:00' as date_day union all 
    
        select '2023-06-26 00:00:00' as date_day union all 
    
        select '2023-06-27 00:00:00' as date_day union all 
    
        select '2023-06-28 00:00:00' as date_day union all 
    
        select '2023-06-29 00:00:00' as date_day union all 
    
        select '2023-06-30 00:00:00' as date_day union all 
    
        select '2023-07-01 00:00:00' as date_day union all 
    
        select '2023-07-02 00:00:00' as date_day union all 
    
        select '2023-07-03 00:00:00' as date_day union all 
    
        select '2023-07-04 00:00:00' as date_day union all 
    
        select '2023-07-05 00:00:00' as date_day union all 
    
        select '2023-07-06 00:00:00' as date_day union all 
    
        select '2023-07-07 00:00:00' as date_day union all 
    
        select '2023-07-08 00:00:00' as date_day union all 
    
        select '2023-07-09 00:00:00' as date_day union all 
    
        select '2023-07-10 00:00:00' as date_day union all 
    
        select '2023-07-11 00:00:00' as date_day union all 
    
        select '2023-07-12 00:00:00' as date_day union all 
    
        select '2023-07-13 00:00:00' as date_day union all 
    
        select '2023-07-14 00:00:00' as date_day union all 
    
        select '2023-07-15 00:00:00' as date_day union all 
    
        select '2023-07-16 00:00:00' as date_day union all 
    
        select '2023-07-17 00:00:00' as date_day union all 
    
        select '2023-07-18 00:00:00' as date_day union all 
    
        select '2023-07-19 00:00:00' as date_day union all 
    
        select '2023-07-20 00:00:00' as date_day union all 
    
        select '2023-07-21 00:00:00' as date_day union all 
    
        select '2023-07-22 00:00:00' as date_day union all 
    
        select '2023-07-23 00:00:00' as date_day union all 
    
        select '2023-07-24 00:00:00' as date_day union all 
    
        select '2023-07-25 00:00:00' as date_day union all 
    
        select '2023-07-26 00:00:00' as date_day union all 
    
        select '2023-07-27 00:00:00' as date_day union all 
    
        select '2023-07-28 00:00:00' as date_day union all 
    
        select '2023-07-29 00:00:00' as date_day union all 
    
        select '2023-07-30 00:00:00' as date_day union all 
    
        select '2023-07-31 00:00:00' as date_day union all 
    
        select '2023-08-01 00:00:00' as date_day union all 
    
        select '2023-08-02 00:00:00' as date_day union all 
    
        select '2023-08-03 00:00:00' as date_day union all 
    
        select '2023-08-04 00:00:00' as date_day union all 
    
        select '2023-08-05 00:00:00' as date_day union all 
    
        select '2023-08-06 00:00:00' as date_day union all 
    
        select '2023-08-07 00:00:00' as date_day union all 
    
        select '2023-08-08 00:00:00' as date_day union all 
    
        select '2023-08-09 00:00:00' as date_day union all 
    
        select '2023-08-10 00:00:00' as date_day union all 
    
        select '2023-08-11 00:00:00' as date_day union all 
    
        select '2023-08-12 00:00:00' as date_day union all 
    
        select '2023-08-13 00:00:00' as date_day union all 
    
        select '2023-08-14 00:00:00' as date_day union all 
    
        select '2023-08-15 00:00:00' as date_day union all 
    
        select '2023-08-16 00:00:00' as date_day union all 
    
        select '2023-08-17 00:00:00' as date_day union all 
    
        select '2023-08-18 00:00:00' as date_day union all 
    
        select '2023-08-19 00:00:00' as date_day union all 
    
        select '2023-08-20 00:00:00' as date_day union all 
    
        select '2023-08-21 00:00:00' as date_day union all 
    
        select '2023-08-22 00:00:00' as date_day union all 
    
        select '2023-08-23 00:00:00' as date_day union all 
    
        select '2023-08-24 00:00:00' as date_day union all 
    
        select '2023-08-25 00:00:00' as date_day union all 
    
        select '2023-08-26 00:00:00' as date_day union all 
    
        select '2023-08-27 00:00:00' as date_day union all 
    
        select '2023-08-28 00:00:00' as date_day union all 
    
        select '2023-08-29 00:00:00' as date_day union all 
    
        select '2023-08-30 00:00:00' as date_day union all 
    
        select '2023-08-31 00:00:00' as date_day union all 
    
        select '2023-09-01 00:00:00' as date_day union all 
    
        select '2023-09-02 00:00:00' as date_day union all 
    
        select '2023-09-03 00:00:00' as date_day union all 
    
        select '2023-09-04 00:00:00' as date_day union all 
    
        select '2023-09-05 00:00:00' as date_day union all 
    
        select '2023-09-06 00:00:00' as date_day union all 
    
        select '2023-09-07 00:00:00' as date_day union all 
    
        select '2023-09-08 00:00:00' as date_day union all 
    
        select '2023-09-09 00:00:00' as date_day union all 
    
        select '2023-09-10 00:00:00' as date_day union all 
    
        select '2023-09-11 00:00:00' as date_day union all 
    
        select '2023-09-12 00:00:00' as date_day union all 
    
        select '2023-09-13 00:00:00' as date_day union all 
    
        select '2023-09-14 00:00:00' as date_day union all 
    
        select '2023-09-15 00:00:00' as date_day union all 
    
        select '2023-09-16 00:00:00' as date_day union all 
    
        select '2023-09-17 00:00:00' as date_day union all 
    
        select '2023-09-18 00:00:00' as date_day union all 
    
        select '2023-09-19 00:00:00' as date_day union all 
    
        select '2023-09-20 00:00:00' as date_day union all 
    
        select '2023-09-21 00:00:00' as date_day union all 
    
        select '2023-09-22 00:00:00' as date_day union all 
    
        select '2023-09-23 00:00:00' as date_day union all 
    
        select '2023-09-24 00:00:00' as date_day union all 
    
        select '2023-09-25 00:00:00' as date_day union all 
    
        select '2023-09-26 00:00:00' as date_day union all 
    
        select '2023-09-27 00:00:00' as date_day union all 
    
        select '2023-09-28 00:00:00' as date_day union all 
    
        select '2023-09-29 00:00:00' as date_day union all 
    
        select '2023-09-30 00:00:00' as date_day union all 
    
        select '2023-10-01 00:00:00' as date_day union all 
    
        select '2023-10-02 00:00:00' as date_day union all 
    
        select '2023-10-03 00:00:00' as date_day union all 
    
        select '2023-10-04 00:00:00' as date_day union all 
    
        select '2023-10-05 00:00:00' as date_day union all 
    
        select '2023-10-06 00:00:00' as date_day union all 
    
        select '2023-10-07 00:00:00' as date_day union all 
    
        select '2023-10-08 00:00:00' as date_day union all 
    
        select '2023-10-09 00:00:00' as date_day union all 
    
        select '2023-10-10 00:00:00' as date_day union all 
    
        select '2023-10-11 00:00:00' as date_day union all 
    
        select '2023-10-12 00:00:00' as date_day union all 
    
        select '2023-10-13 00:00:00' as date_day union all 
    
        select '2023-10-14 00:00:00' as date_day union all 
    
        select '2023-10-15 00:00:00' as date_day union all 
    
        select '2023-10-16 00:00:00' as date_day union all 
    
        select '2023-10-17 00:00:00' as date_day union all 
    
        select '2023-10-18 00:00:00' as date_day union all 
    
        select '2023-10-19 00:00:00' as date_day union all 
    
        select '2023-10-20 00:00:00' as date_day union all 
    
        select '2023-10-21 00:00:00' as date_day union all 
    
        select '2023-10-22 00:00:00' as date_day union all 
    
        select '2023-10-23 00:00:00' as date_day union all 
    
        select '2023-10-24 00:00:00' as date_day union all 
    
        select '2023-10-25 00:00:00' as date_day union all 
    
        select '2023-10-26 00:00:00' as date_day union all 
    
        select '2023-10-27 00:00:00' as date_day union all 
    
        select '2023-10-28 00:00:00' as date_day union all 
    
        select '2023-10-29 00:00:00' as date_day union all 
    
        select '2023-10-30 00:00:00' as date_day union all 
    
        select '2023-10-31 00:00:00' as date_day union all 
    
        select '2023-11-01 00:00:00' as date_day union all 
    
        select '2023-11-02 00:00:00' as date_day union all 
    
        select '2023-11-03 00:00:00' as date_day union all 
    
        select '2023-11-04 00:00:00' as date_day union all 
    
        select '2023-11-05 00:00:00' as date_day union all 
    
        select '2023-11-06 00:00:00' as date_day union all 
    
        select '2023-11-07 00:00:00' as date_day union all 
    
        select '2023-11-08 00:00:00' as date_day union all 
    
        select '2023-11-09 00:00:00' as date_day union all 
    
        select '2023-11-10 00:00:00' as date_day union all 
    
        select '2023-11-11 00:00:00' as date_day union all 
    
        select '2023-11-12 00:00:00' as date_day union all 
    
        select '2023-11-13 00:00:00' as date_day union all 
    
        select '2023-11-14 00:00:00' as date_day union all 
    
        select '2023-11-15 00:00:00' as date_day union all 
    
        select '2023-11-16 00:00:00' as date_day union all 
    
        select '2023-11-17 00:00:00' as date_day union all 
    
        select '2023-11-18 00:00:00' as date_day union all 
    
        select '2023-11-19 00:00:00' as date_day union all 
    
        select '2023-11-20 00:00:00' as date_day union all 
    
        select '2023-11-21 00:00:00' as date_day union all 
    
        select '2023-11-22 00:00:00' as date_day union all 
    
        select '2023-11-23 00:00:00' as date_day union all 
    
        select '2023-11-24 00:00:00' as date_day union all 
    
        select '2023-11-25 00:00:00' as date_day union all 
    
        select '2023-11-26 00:00:00' as date_day union all 
    
        select '2023-11-27 00:00:00' as date_day union all 
    
        select '2023-11-28 00:00:00' as date_day union all 
    
        select '2023-11-29 00:00:00' as date_day union all 
    
        select '2023-11-30 00:00:00' as date_day union all 
    
        select '2023-12-01 00:00:00' as date_day union all 
    
        select '2023-12-02 00:00:00' as date_day union all 
    
        select '2023-12-03 00:00:00' as date_day union all 
    
        select '2023-12-04 00:00:00' as date_day union all 
    
        select '2023-12-05 00:00:00' as date_day union all 
    
        select '2023-12-06 00:00:00' as date_day union all 
    
        select '2023-12-07 00:00:00' as date_day union all 
    
        select '2023-12-08 00:00:00' as date_day union all 
    
        select '2023-12-09 00:00:00' as date_day union all 
    
        select '2023-12-10 00:00:00' as date_day union all 
    
        select '2023-12-11 00:00:00' as date_day union all 
    
        select '2023-12-12 00:00:00' as date_day union all 
    
        select '2023-12-13 00:00:00' as date_day union all 
    
        select '2023-12-14 00:00:00' as date_day union all 
    
        select '2023-12-15 00:00:00' as date_day union all 
    
        select '2023-12-16 00:00:00' as date_day union all 
    
        select '2023-12-17 00:00:00' as date_day union all 
    
        select '2023-12-18 00:00:00' as date_day union all 
    
        select '2023-12-19 00:00:00' as date_day union all 
    
        select '2023-12-20 00:00:00' as date_day union all 
    
        select '2023-12-21 00:00:00' as date_day union all 
    
        select '2023-12-22 00:00:00' as date_day union all 
    
        select '2023-12-23 00:00:00' as date_day union all 
    
        select '2023-12-24 00:00:00' as date_day union all 
    
        select '2023-12-25 00:00:00' as date_day union all 
    
        select '2023-12-26 00:00:00' as date_day union all 
    
        select '2023-12-27 00:00:00' as date_day union all 
    
        select '2023-12-28 00:00:00' as date_day union all 
    
        select '2023-12-29 00:00:00' as date_day union all 
    
        select '2023-12-30 00:00:00' as date_day 
    

)
    
,base_dates as (
    
    
select
    cast(d.date_day as 
    
    
    DATETIMEOFFSET
) as date_day
from
    date_spine d


),
dates_with_prior_year_dates as (

    select
        cast(d.date_day as date) as date_day,
        cast(

    dateadd(
        year,
        -1,
        cast(d.date_day as datetime)
        )

 as date) as prior_year_date_day,
        cast(

    dateadd(
        day,
        -364,
        cast(d.date_day as datetime)
        )

 as date) as prior_year_over_year_date_day
    from
    	base_dates d

)
select
    d.date_day,
    cast(

    dateadd(
        day,
        -1,
        cast(d.date_day as datetime)
        )

 as date) as prior_date_day,
    cast(

    dateadd(
        day,
        1,
        cast(d.date_day as datetime)
        )

 as date) as next_date_day,
    d.prior_year_date_day as prior_year_date_day,
    d.prior_year_over_year_date_day,
    datepart(weekday, d.date_day) as day_of_week,
    case
        -- Shift start of week from Sunday (1) to Monday (2)
        when datepart(weekday, d.date_day) = 1 then 7
        else datepart(weekday, d.date_day) - 1
    end as day_of_week_iso,
    format(d.date_day, 'dddd') as day_of_week_name,
    format(d.date_day, 'ddd') as day_of_week_name_short,
    datepart(day, d.date_day) as day_of_month,
    datepart(dayofyear, d.date_day) as day_of_year,

    -- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        cast(
    CAST(DATEADD(week, DATEDIFF(week, 0, 

    dateadd(
        day,
        1,
        cast(d.date_day as datetime)
        )

), 0) AS DATE)
 as datetime)
        )

 as date) as week_start_date,
    cast(

    dateadd(
        day,
        6,
        cast(-- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        cast(
    CAST(DATEADD(week, DATEDIFF(week, 0, 

    dateadd(
        day,
        1,
        cast(d.date_day as datetime)
        )

), 0) AS DATE)
 as datetime)
        )

 as date) as datetime)
        )

 as date) as week_end_date,
    -- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        cast(
    CAST(DATEADD(week, DATEDIFF(week, 0, 

    dateadd(
        day,
        1,
        cast(d.prior_year_over_year_date_day as datetime)
        )

), 0) AS DATE)
 as datetime)
        )

 as date) as prior_year_week_start_date,
    cast(

    dateadd(
        day,
        6,
        cast(-- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        cast(
    CAST(DATEADD(week, DATEDIFF(week, 0, 

    dateadd(
        day,
        1,
        cast(d.prior_year_over_year_date_day as datetime)
        )

), 0) AS DATE)
 as datetime)
        )

 as date) as datetime)
        )

 as date) as prior_year_week_end_date,
    cast(datepart(week, d.date_day) as 
    int
) as week_of_year,

    cast(dateadd(week, datediff(week, 0, dateadd(day, -1, d.date_day)), 0) as date) as iso_week_start_date,
    cast(

    dateadd(
        day,
        6,
        cast(cast(dateadd(week, datediff(week, 0, dateadd(day, -1, d.date_day)), 0) as date) as datetime)
        )

 as date) as iso_week_end_date,
    cast(dateadd(week, datediff(week, 0, dateadd(day, -1, d.prior_year_over_year_date_day)), 0) as date) as prior_year_iso_week_start_date,
    cast(

    dateadd(
        day,
        6,
        cast(cast(dateadd(week, datediff(week, 0, dateadd(day, -1, d.prior_year_over_year_date_day)), 0) as date) as datetime)
        )

 as date) as prior_year_iso_week_end_date,
    cast(datepart(iso_week, d.date_day) as 
    int
) as iso_week_of_year,

    cast(datepart(week, d.prior_year_over_year_date_day) as 
    int
) as prior_year_week_of_year,
    cast(datepart(iso_week, d.prior_year_over_year_date_day) as 
    int
) as prior_year_iso_week_of_year,

    cast(datepart(month, d.date_day) as 
    int
) as month_of_year,
    format(d.date_day, 'MMMM')  as month_name,
    format(d.date_day, 'MMM')  as month_name_short,

    cast(
    CAST(DATEADD(month, DATEDIFF(month, 0, d.date_day), 0) AS DATE)
 as date) as month_start_date,
    cast(EOMONTH ( d.date_day) as date) as month_end_date,

    cast(
    CAST(DATEADD(month, DATEDIFF(month, 0, d.prior_year_date_day), 0) AS DATE)
 as date) as prior_year_month_start_date,
    cast(EOMONTH ( d.prior_year_date_day) as date) as prior_year_month_end_date,

    cast(datepart(quarter, d.date_day) as 
    int
) as quarter_of_year,
    cast(
    CAST(DATEADD(quarter, DATEDIFF(quarter, 0, d.date_day), 0) AS DATE)
 as date) as quarter_start_date,
    cast(CAST(DATEADD(QUARTER, DATEDIFF(QUARTER, 0, d.date_day) + 1, -1) AS DATE) as date) as quarter_end_date,

    cast(datepart(year, d.date_day) as 
    int
) as year_number,
    cast(
    CAST(DATEADD(year, DATEDIFF(year, 0, d.date_day), 0) AS DATE)
 as date) as year_start_date,
    cast(CAST(DATEADD(YEAR, DATEDIFF(year, 0, d.date_day) + 1, -1) AS DATE) as date) as year_end_date
from
    dates_with_prior_year_dates d
