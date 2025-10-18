class Record:
    def __init__(self, restaurant_id, restaurant_name, country_code, city, address, 
                 locality, locality_verbose, longitude, latitude, cuisines, 
                 average_cost_for_two, currency, has_table_booking, has_online_delivery,
                 is_delivering_now, switch_to_order_menu, price_range, aggregate_rating,
                 rating_color, rating_text, votes):
        self.restaurant_id = restaurant_id
        self.restaurant_name = restaurant_name
        self.country_code = country_code
        self.city = city
        self.address = address
        self.locality = locality
        self.locality_verbose = locality_verbose
        self.longitude = longitude
        self.latitude = latitude
        self.cuisines = cuisines
        self.average_cost_for_two = average_cost_for_two
        self.currency = currency
        self.has_table_booking = has_table_booking
        self.has_online_delivery = has_online_delivery
        self.is_delivering_now = is_delivering_now
        self.switch_to_order_menu = switch_to_order_menu
        self.price_range = price_range
        self.aggregate_rating = aggregate_rating
        self.rating_color = rating_color
        self.rating_text = rating_text
        self.votes = votes