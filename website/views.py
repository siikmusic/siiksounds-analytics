from flask import Blueprint, render_template, request, flash, redirect
import pandas as pd
import csv, sqlite3
from datetime import datetime
import numpy as np
import requests
import json
from website.static.countries import countryMap as country_codes
views = Blueprint('views', __name__)

PAYPAL_RATES = 0.980728390317
STRIPE_NON_EU = 0.962
STRIPE_EU = 0.979551012
SHOPIFY = 217.57
EURO_COUNTRIES = {"Austria",
                  "Belgium",
                  "Croatia",
                  "Cyprus",
                  "Estonia",
                  "Finland",
                  "France",
                  "Germany",
                  "Greece",
                  "Ireland",
                  "Italy",
                  "Latvia",
                  "Lithuania",
                  "Luxembourg",
                  "Malta",
                  "Netherlands",
                  "Portugal",
                  "Slovakia",
                  "Slovenia",
                  "Spain"}
country=['Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Republic of Cyprus', 'Czech Republic',
'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland', 'Italy',
'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland', 'Portugal', 'Romania',
'Slovakia', 'Slovenia', 'Spain' , 'Sweden']

MONTHS = {
    0: "Jan",
    1: "Feb",
    2: "Mar",
    3: "Apr",
    4: "May",
    5: "June",
    6: "July",
    7: "Aug",
    8: "Sep",
    9: "Oct",
    10: "Nov",
    11: "Dec",
}


@views.route('/', methods=['GET', 'POST'])
def payouts():
    if request.method == "GET":
        return render_template(
            "payouts_form.html"
        )
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    sales = request.files['file']
    payments = request.files['file_payments']
    ad_ss = float(request.form.get("ad-expenses"))
    ad_doxed = float(request.form.get("ad-expenses-doxed"))
    store_expenses = float(request.form.get("store-expenses"))

    if sales.filename == '' or payments.filename == '':
        flash('No selected file')
        return redirect(request.url)
    if sales and allowed_file(sales.filename) and payments and allowed_file(payments.filename):
        df_sales = pd.read_csv(sales)
        df_payments = pd.read_csv(payments)
        conn = sqlite3.connect("identifier.sqlite")
        df_sales_new = pd.merge(df_sales, df_payments, on='Order')
        df_sales_new = df_sales_new.drop_duplicates(subset=["Sale ID"], keep=False)
        df_sales_new.to_sql("earnings", conn, if_exists='append', index=False)
        datestring = pd.to_datetime(df_sales["Date"].values[:1])
        year = datestring.year.values[0].astype(str)
        month = datestring.month.values[0].astype(str)
        mycursor = conn.cursor()
        try:
            mycursor.execute(
                "REPLACE INTO expenses (year, month, ads_ss, ads_doxed, store_expenses) VALUES (?, ?, ?,?,?)",
                (year, month, ad_ss, ad_doxed, store_expenses))
            conn.commit()
        except:
            print("exists")
        conn.close()
        flash('Success!')
        return redirect(request.url)

    flash('Not a CSV file')
    return redirect(request.url)

@views.route('/test', methods=['GET', 'POST'])
def test():
    return render_template(
        "test.html"
    )
@views.route('/analytics', methods=['GET', 'POST'])
def analytics():
    conn = sqlite3.connect("identifier.sqlite")
    cur = conn.cursor()
    cur.execute("SELECT * FROM expenses")
    date_data = []
    rows = cur.fetchall()
    for row in rows:
        date_data.append([row[0], MONTHS[int(row[1]) - 1]])
    cur = conn.cursor()
    dates_placeholder=""
    month_from = 0
    month_to = 0
    year_from = 0
    year_to = 0
    if request.method == "GET":
        data = pd.read_sql(sql="SELECT * FROM earnings",con=conn)
    else:
        dates_placeholder = request.form.get("dates")
        dates = request.form.get("dates").split(" to ")
        if len(dates) == 1:
            flash('Error')
            return render_template(
                "analytics_error.html"
            )
        date_from = dates[0].split("/")
        year_from = int("20"+date_from[2])
        month_from = int(date_from[1])
        date_from = "20"+date_from[2]+"-"+date_from[1]+"-"+date_from[0]
        date_to = dates[1].split("/")
        year_to = int("20"+date_to[2])
        month_to = int(date_to[1])
        date_to = "20"+date_to[2]+"-"+date_to[1]+"-"+date_to[0]
        print(date_from,date_to)
        data = pd.read_sql(sql="SELECT * FROM earnings WHERE strftime('%Y-%m-%d', Date_x) BETWEEN ? AND ?", con=conn, params=(date_from,date_to))
    df = pd.DataFrame(data)
    if not df.empty:
        total_sales = df['Total sales'].sum()
        df['In Eu'] = df.apply(lambda x: x['Billing country'] in country, axis=1)
        result = df.groupby("In Eu").sum()
        in_eu = result.iloc[1]['Net sales']
        not_in_eu = result.iloc[0]['Net sales']
        eu_data = [in_eu,not_in_eu]

        df = pd.DataFrame(data)
        df = df.groupby("Billing country").sum()
        df = df.sort_values(['Total sales'], ascending=False)
        df = df.head(10)
        countries = list(df.index.values)
        sales_by_country = df["Total sales"].tolist()
        country_data = []
        for i in range(len(countries)):
            country_data.append((countries[i], sales_by_country[i],"https://flagcdn.com/16x12/"+country_codes[countries[i]].lower()+".png"))

        df = pd.DataFrame(data)
        df = df.groupby("Billing city").sum()
        df = df.sort_values(['Total sales'], ascending=False)
        df = df.head(10)
        cities = list(df.index.values)
        sales_by_city = df["Total sales"].tolist()
        city_data = [cities,sales_by_city]

        df = pd.DataFrame(data)
        df = df.groupby("Product").sum()
        df = df.sort_values(['Total sales'], ascending=False)
        df = df.head(10)
        products = list(df.index.values)
        sales_by_product = df["Total sales"].tolist()
        product_data = []
        for i in range(len(products)):
            product_data.append((products[i], sales_by_product[i]))
        cur = conn.cursor()
        if request.method == "POST":
            cur.execute("SELECT * FROM expenses WHERE CAST(year AS INTEGER) >= ? AND CAST(year AS INTEGER) <= ? AND CAST(month AS INTEGER) >= ? AND CAST(month AS INTEGER) <= ?",(year_from,year_to,month_from,month_to))
        else:
            cur.execute("SELECT * FROM expenses")
        rows = cur.fetchall()
        ad_expenses = 0
        for row in rows:
            ad_expenses += row[2]
            ad_expenses += row[3]
            ad_expenses += row[4]
        df = pd.DataFrame(data)
        stripe_eu_total, stripe_non_eu_total, total_paypal, total_earnings = calculate_from_df(df)
        total_store = SHOPIFY + total_earnings * 0.02
        total_stripe = stripe_non_eu_total + stripe_eu_total

        total_expenses_payment = total_stripe + total_paypal
        total_net = total_earnings - total_stripe - total_paypal - total_store - ad_expenses

        df = pd.DataFrame(data)
        df["Product"] = df['Product'].str.split(' - ', n=1)
        df["Product"] = df['Product'].apply(lambda x: x if x is None else x[0])
        df["Product"] = df['Product'].apply(lambda x: "SLCTD collections." if x =="SLCTD collections" else x)
        df["Product"] = df['Product'].apply(lambda x: "Sickrate & SIIK Essentials I" if x =="Sickrate & SIIK Essentials" else x)

        df = df.groupby("Product").sum()
        df = df.sort_values(['Total sales'], ascending=False)
        products = list(df.index.values)
        sales_by_product = df["Total sales"].tolist()
        product_data_merged = []
        for i in range(len(products)):
            product_data_merged.append((products[i], sales_by_product[i]))
        return render_template(
        "analytics.html", date_data=date_data,eu_data=eu_data,country_data=country_data,city_data=city_data,total_sales=total_sales, product_data=product_data,dates=dates_placeholder,total_earnings=total_earnings,total_net=total_net,product_data_merged=product_data_merged,total_expenses_payment=total_expenses_payment,total_store=total_store
    )
    else:
        flash('No purchases in date range')

        return render_template(
            "analytics_error.html"
        )


@views.route('/payouts', methods=['GET', 'POST'])
def payouts_description():
    conn = sqlite3.connect("identifier.sqlite")
    args = request.args
    year = args.get("year", default="", type=str)
    month = args.get("month", default="", type=str)
    month = str(list(MONTHS.keys())[list(MONTHS.values()).index(month)] + 1)

    cur = conn.cursor()
    cur.execute("SELECT * FROM expenses WHERE year = ? AND month = ?", (year, month))
    rows = cur.fetchone()
    ad_ss = float(rows[2])
    ad_doxed = float(rows[3])

    store_expenses = float(rows[4])

    month_clean = month
    if len(month) < 2:
        month_clean = "0" + month_clean
    print(year, month)
    data = pd.read_sql(sql="SELECT * FROM earnings WHERE strftime('%Y', Date_x) = ? AND strftime('%m',Date_x) = ?",
                       con=conn, params=(year, month_clean,))

    df = pd.DataFrame(data)
    df_sickrate = df[df["Product"].str.contains("Sickrate", na=False) | df["Product"].str.contains("SLCTD", na=False)]
    stripe_eu_total_sickrate, stripe_non_eu_total_sickrate,paypal_total_sickrate,earnings_sickrate = calculate_from_df(df_sickrate)
    store_expenses_sickrate = earnings_sickrate*0.02/2 + SHOPIFY/2 + store_expenses/2

    df_doxed = df[df["Product"].str.contains("Doxed", na=False)]
    stripe_eu_total_doxed, stripe_non_eu_total_doxed,paypal_total_doxed,earnings_doxed = calculate_from_df(df_doxed)
    store_expenses_doxed = earnings_doxed*0.02/2
    store_expenses_siik = earnings_doxed*0.02/2  + earnings_sickrate*0.02/2  + SHOPIFY/2

    total_payout_sickrate = (earnings_sickrate - stripe_eu_total_sickrate - stripe_non_eu_total_sickrate - paypal_total_sickrate - ad_ss - store_expenses_sickrate) / 2

    total_payout_doxed = (earnings_doxed - stripe_eu_total_doxed - stripe_non_eu_total_doxed - paypal_total_doxed - ad_doxed - store_expenses_doxed) / 2
    total_payout_siik = total_payout_sickrate+total_payout_doxed

    stripe_eu_total, stripe_non_eu_total,total_paypal,total_earnings =calculate_from_df(df)
    total_store = SHOPIFY + total_earnings*0.02
    total_stripe = stripe_non_eu_total + stripe_eu_total
    total_net = total_earnings - total_stripe - total_paypal - total_store - ad_doxed -ad_ss
    return render_template(
        "payouts_data.html",
        store_expenses_sickrate=round(store_expenses_sickrate,2),
        ad_ss=ad_ss,
        earnings_sickrate=round(earnings_sickrate,2),
        stripe_eu_total_sickrate=round(stripe_eu_total_sickrate/2,2),
        stripe_non_eu_total_sickrate=round(stripe_non_eu_total_sickrate/2,2),
        paypal_total_sickrate=round(paypal_total_sickrate/2,2),
        total_payout_sickrate=round(total_payout_sickrate,2),
        store_expenses_doxed=round(store_expenses_doxed/2,2),
        ad_doxed=round(ad_doxed,2),
        earnings_doxed=round(earnings_doxed,2),
        stripe_eu_total_doxed=round(stripe_eu_total_doxed/2,2),
        stripe_non_eu_total_doxed=round(stripe_non_eu_total_doxed/2,2),
        paypal_total_doxed=round(paypal_total_doxed/2,2),
        total_payout_doxed=round(total_payout_doxed*0.8,2),
        store_expenses_siik=round(store_expenses_siik,2),
        total_payout_siik=round(total_payout_siik,2),
        month=args.get("month", default="", type=str),
        year=args.get("year", default="", type=str),
        total_net=round(total_net,2),
        total_earnings=round(total_earnings,2),
        total_stripe=round(total_stripe,2),
        total_paypal=round(total_paypal,2),
        total_store=round(total_store,2)
    )
def calculate_from_df(df):
    df_paypal = df[df["Payment type"].str.contains("PayPal", na=False)]
    df_stripe = df[df["Payment type"].str.contains("Stripe", na=False)]
    df_stripe_eu = df_stripe[df_stripe["Billing country"].isin(EURO_COUNTRIES)]
    df_stripe_non_eu = df_stripe[df_stripe["Billing country"].isin(EURO_COUNTRIES) == False]

    stripe_eu_total = df_stripe_eu['Total sales'].sum() - df_stripe_eu['Total sales'].sum() * STRIPE_EU
    stripe_non_eu_total = df_stripe_non_eu['Total sales'].sum() - df_stripe_non_eu['Total sales'].sum() * STRIPE_NON_EU
    paypal_total = df_paypal['Total sales'].sum() - df_paypal['Total sales'].sum() * PAYPAL_RATES
    earnings = df["Total sales"].sum()

    return (stripe_eu_total,stripe_non_eu_total,paypal_total,earnings)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in {'csv'}
