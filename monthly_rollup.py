from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_fiscal_year(df, date_feild, fiscal_start_month):
    """
    Calculate the fiscal year from date

    :param df: Source Dataframe.
    :param date_field: Date column.
    :return: Dataframe with the year columns.
    """
    fiscal_year_expr = F.when(F.month(date_feild) >= fiscal_start_month, F.year(date_feild) + 1).otherwise( F.year(date_feild))
    return  df.withColumn("FiscalYear", fiscal_year_expr)

def create_fiscal_month(df, date_feild, fiscal_start_month):
    """
    Calculate the fiscal month from date

    :param df: Source Dataframe.
    :param date_field: Date column.
    :return: Dataframe with the month columns.
    """
    fiscal_month_expr = F.when(F.month(date_feild) >= fiscal_start_month, F.month(date_feild) - 9).otherwise( F.month(date_feild) + 3)
    return  df.withColumn("FiscalMonth", fiscal_month_expr)

def monthly_rolling_average(df, group_fields, date_feild, metric_feild):
    """
    Calculate 6 month rolling average using window specifications for monthly data.

    :param df: Source Dataframe.
    :param group_field: Field to partition over.
    :param date_field: Date column to order by within each partition.
    :param metirc_field: Matric column to calculate the average.
    :return: Dataframe with the rolling average column added.
    """
    # Defined window spec: 5 month lookback to include current month
    window_spec = Window.partitionBy(group_fields).orderBy(date_feild).rowsBetween(-5, 0)

    return df.withColumn(f"{metric_feild}_SixMonthMovingAvg", F.ceil(F.avg(metric_feild).over(window=window_spec)))

def fiscal_year_rolling_monthly_average(df, group_fields, metric_feild):
    """
    Calculate annual average using window specifications for monthly data.

    :param df: Source Dataframe.
    :param group_field: Field to partition over.
    :param year_feild: Year column to order by within each partition.
    :param metirc_field: Matric column to calculate the average.
    :return: Dataframe with the annual average column added.
    """
    
    # Defined window spec: 5 month lookback to include current month
    window_spec = Window.partitionBy(group_fields)
    return df.withColumn(f"{metric_feild}_RollingMonthlyAvg", F.ceil(F.avg(metric_feild).over(window=window_spec)))

def annulized_monthly_rate(df, date_field, metric_field):
    """
    Calculate annualize rate of a monthly metric.

    :param df: Source Dataframe.
    :param date_field: Date column to order by within each partition.
    :param metirc_field: Matric column to calculate the annualized rate.
    :return: Dataframe with the annualized rate column added.
    """
    # Calculate the number of days in each month
    days_in_month_expr = F.dayofmonth(F.last_day(F.col(date_field)))

    # Calculate total days in a year
    is_leap_year_expr = (F.year(F.col(date_field)) % 4 == 0)
    total_days_in_year_expr = F.when(is_leap_year_expr, 366).otherwise(365)

    # Annualize the metric considering the number of day in each month
    annualized_rate_expr = (F.col(metric_field) / days_in_month_expr) * total_days_in_year_expr

    return df.withColumn(f"{metric_field}_Annualized", F.ceil(annualized_rate_expr))

def annual_rolling_metrics(df, group_fields, date_feild, fiscal_year_field, fiscal_month, metric_feild):
    """
    Calculate rolling total and rolling monthly average for the current year, 
    grouped by feilds.

    :param df: Source Dataframe.
    :param group_field: Field to partition over.
    :param date_field: Date column to order by within each partition.
    :param metirc_field: Matric column to calculate the average.
    :return: Dataframe with the rolling total and rolling average (2) columns added.
    """
    return
