package bdt.hotel.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class HotelDTO implements Writable {
	 	String key;
		String hotel;
		int	is_canceled;
		int lead_timetime;
		int	arrival_date_year;
		int	arrival_date_month;
		int	arrival_date_week_number;
		int	arrival_date_day_of_month;
		int	stays_in_weekend_nights;
		int	stays_in_week_nights;
		int	adults;
		int	children;
		int	babies;
		String	meal;
		String	country;
		String	market_segment;
		String	distribution_channel;
		int	is_repeated_guest;
		int previous_cancellations; 
		int previous_bookings_not_canceled;
		int reserved_room_type;
		String		assigned_room_type;
		int booking_changes;
		String deposit_type;
		int agent;
		int company;
		int days_in_waiting_list;
		String customer_type;
		double adr;
		int required_car_parking_spaces;
		int total_of_special_requests;
		String reservation_status;
		String reservation_status_date;

		
	public HotelDTO(String key, String hotel, int is_canceled,
				int lead_timetime, int arrival_date_year,
				int arrival_date_month, int arrival_date_week_number,
				int arrival_date_day_of_month, int stays_in_weekend_nights,
				int stays_in_week_nights, int adults, int children, int babies,
				String meal, String country, String market_segment,
				String distribution_channel, int is_repeated_guest,
				int previous_cancellations, int previous_bookings_not_canceled,
				int reserved_room_type, String assigned_room_type,
				int booking_changes, String deposit_type, int agent,
				int company, int days_in_waiting_list, String customer_type,
				double adr, int required_car_parking_spaces,
				int total_of_special_requests, String reservation_status,
				String reservation_status_date) {
			super();
			this.key = key;
			this.hotel = hotel;
			this.is_canceled = is_canceled;
			this.lead_timetime = lead_timetime;
			this.arrival_date_year = arrival_date_year;
			this.arrival_date_month = arrival_date_month;
			this.arrival_date_week_number = arrival_date_week_number;
			this.arrival_date_day_of_month = arrival_date_day_of_month;
			this.stays_in_weekend_nights = stays_in_weekend_nights;
			this.stays_in_week_nights = stays_in_week_nights;
			this.adults = adults;
			this.children = children;
			this.babies = babies;
			this.meal = meal;
			this.country = country;
			this.market_segment = market_segment;
			this.distribution_channel = distribution_channel;
			this.is_repeated_guest = is_repeated_guest;
			this.previous_cancellations = previous_cancellations;
			this.previous_bookings_not_canceled = previous_bookings_not_canceled;
			this.reserved_room_type = reserved_room_type;
			this.assigned_room_type = assigned_room_type;
			this.booking_changes = booking_changes;
			this.deposit_type = deposit_type;
			this.agent = agent;
			this.company = company;
			this.days_in_waiting_list = days_in_waiting_list;
			this.customer_type = customer_type;
			this.adr = adr;
			this.required_car_parking_spaces = required_car_parking_spaces;
			this.total_of_special_requests = total_of_special_requests;
			this.reservation_status = reservation_status;
			this.reservation_status_date = reservation_status_date;
		}

	
	
	public String getKey() {
		return key;
	}



	public void setKey(String key) {
		this.key = key;
	}



	public String getHotel() {
		return hotel;
	}



	public void setHotel(String hotel) {
		this.hotel = hotel;
	}



	public int getIs_canceled() {
		return is_canceled;
	}



	public void setIs_canceled(int is_canceled) {
		this.is_canceled = is_canceled;
	}



	public int getLead_timetime() {
		return lead_timetime;
	}



	public void setLead_timetime(int lead_timetime) {
		this.lead_timetime = lead_timetime;
	}



	public int getArrival_date_year() {
		return arrival_date_year;
	}



	public void setArrival_date_year(int arrival_date_year) {
		this.arrival_date_year = arrival_date_year;
	}



	public int getArrival_date_month() {
		return arrival_date_month;
	}



	public void setArrival_date_month(int arrival_date_month) {
		this.arrival_date_month = arrival_date_month;
	}



	public int getArrival_date_week_number() {
		return arrival_date_week_number;
	}



	public void setArrival_date_week_number(int arrival_date_week_number) {
		this.arrival_date_week_number = arrival_date_week_number;
	}



	public int getArrival_date_day_of_month() {
		return arrival_date_day_of_month;
	}



	public void setArrival_date_day_of_month(int arrival_date_day_of_month) {
		this.arrival_date_day_of_month = arrival_date_day_of_month;
	}



	public int getStays_in_weekend_nights() {
		return stays_in_weekend_nights;
	}



	public void setStays_in_weekend_nights(int stays_in_weekend_nights) {
		this.stays_in_weekend_nights = stays_in_weekend_nights;
	}



	public int getStays_in_week_nights() {
		return stays_in_week_nights;
	}



	public void setStays_in_week_nights(int stays_in_week_nights) {
		this.stays_in_week_nights = stays_in_week_nights;
	}



	public int getAdults() {
		return adults;
	}



	public void setAdults(int adults) {
		this.adults = adults;
	}



	public int getChildren() {
		return children;
	}



	public void setChildren(int children) {
		this.children = children;
	}



	public int getBabies() {
		return babies;
	}



	public void setBabies(int babies) {
		this.babies = babies;
	}



	public String getMeal() {
		return meal;
	}



	public void setMeal(String meal) {
		this.meal = meal;
	}



	public String getCountry() {
		return country;
	}



	public void setCountry(String country) {
		this.country = country;
	}



	public String getMarket_segment() {
		return market_segment;
	}



	public void setMarket_segment(String market_segment) {
		this.market_segment = market_segment;
	}



	public String getDistribution_channel() {
		return distribution_channel;
	}



	public void setDistribution_channel(String distribution_channel) {
		this.distribution_channel = distribution_channel;
	}



	public int getIs_repeated_guest() {
		return is_repeated_guest;
	}



	public void setIs_repeated_guest(int is_repeated_guest) {
		this.is_repeated_guest = is_repeated_guest;
	}



	public int getPrevious_cancellations() {
		return previous_cancellations;
	}



	public void setPrevious_cancellations(int previous_cancellations) {
		this.previous_cancellations = previous_cancellations;
	}



	public int getPrevious_bookings_not_canceled() {
		return previous_bookings_not_canceled;
	}



	public void setPrevious_bookings_not_canceled(int previous_bookings_not_canceled) {
		this.previous_bookings_not_canceled = previous_bookings_not_canceled;
	}



	public int getReserved_room_type() {
		return reserved_room_type;
	}



	public void setReserved_room_type(int reserved_room_type) {
		this.reserved_room_type = reserved_room_type;
	}



	public String getAssigned_room_type() {
		return assigned_room_type;
	}



	public void setAssigned_room_type(String assigned_room_type) {
		this.assigned_room_type = assigned_room_type;
	}



	public int getBooking_changes() {
		return booking_changes;
	}



	public void setBooking_changes(int booking_changes) {
		this.booking_changes = booking_changes;
	}



	public String getDeposit_type() {
		return deposit_type;
	}



	public void setDeposit_type(String deposit_type) {
		this.deposit_type = deposit_type;
	}



	public int getAgent() {
		return agent;
	}



	public void setAgent(int agent) {
		this.agent = agent;
	}



	public int getCompany() {
		return company;
	}



	public void setCompany(int company) {
		this.company = company;
	}



	public int getDays_in_waiting_list() {
		return days_in_waiting_list;
	}



	public void setDays_in_waiting_list(int days_in_waiting_list) {
		this.days_in_waiting_list = days_in_waiting_list;
	}



	public String getCustomer_type() {
		return customer_type;
	}



	public void setCustomer_type(String customer_type) {
		this.customer_type = customer_type;
	}



	public double getAdr() {
		return adr;
	}



	public void setAdr(double adr) {
		this.adr = adr;
	}



	public int getRequired_car_parking_spaces() {
		return required_car_parking_spaces;
	}



	public void setRequired_car_parking_spaces(int required_car_parking_spaces) {
		this.required_car_parking_spaces = required_car_parking_spaces;
	}



	public int getTotal_of_special_requests() {
		return total_of_special_requests;
	}



	public void setTotal_of_special_requests(int total_of_special_requests) {
		this.total_of_special_requests = total_of_special_requests;
	}



	public String getReservation_status() {
		return reservation_status;
	}



	public void setReservation_status(String reservation_status) {
		this.reservation_status = reservation_status;
	}



	public String getReservation_status_date() {
		return reservation_status_date;
	}



	public void setReservation_status_date(String reservation_status_date) {
		this.reservation_status_date = reservation_status_date;
	}



	@Override
	public String toString() {
		return "HotelDTO [key=" + key + ", hotel=" + hotel + ", is_canceled="
				+ is_canceled + ", lead_timetime=" + lead_timetime
				+ ", arrival_date_year=" + arrival_date_year
				+ ", arrival_date_month=" + arrival_date_month
				+ ", arrival_date_week_number=" + arrival_date_week_number
				+ ", arrival_date_day_of_month=" + arrival_date_day_of_month
				+ ", stays_in_weekend_nights=" + stays_in_weekend_nights
				+ ", stays_in_week_nights=" + stays_in_week_nights
				+ ", adults=" + adults + ", children=" + children + ", babies="
				+ babies + ", meal=" + meal + ", country=" + country
				+ ", market_segment=" + market_segment
				+ ", distribution_channel=" + distribution_channel
				+ ", is_repeated_guest=" + is_repeated_guest
				+ ", previous_cancellations=" + previous_cancellations
				+ ", previous_bookings_not_canceled="
				+ previous_bookings_not_canceled + ", reserved_room_type="
				+ reserved_room_type + ", assigned_room_type="
				+ assigned_room_type + ", booking_changes=" + booking_changes
				+ ", deposit_type=" + deposit_type + ", agent=" + agent
				+ ", company=" + company + ", days_in_waiting_list="
				+ days_in_waiting_list + ", customer_type=" + customer_type
				+ ", adr=" + adr + ", required_car_parking_spaces="
				+ required_car_parking_spaces + ", total_of_special_requests="
				+ total_of_special_requests + ", reservation_status="
				+ reservation_status + ", reservation_status_date="
				+ reservation_status_date + "]";
	}



	@Override
	public void readFields(DataInput data) throws IOException {
		key = data.readUTF();
		hotel = data.readUTF();
		is_canceled = data.readInt();
		lead_timetime = data.readInt();
		arrival_date_year = data.readInt();
		arrival_date_month = data.readInt();
		arrival_date_week_number = data.readInt();
		arrival_date_day_of_month = data.readInt();
		stays_in_weekend_nights = data.readInt();
		stays_in_week_nights = data.readInt();
		adults = data.readInt();
		children = data.readInt();
		babies = data.readInt();
		meal = data.readUTF();
		country = data.readUTF();
		market_segment = data.readUTF();
		distribution_channel = data.readUTF();
		is_repeated_guest = data.readInt();
		previous_cancellations = data.readInt();
		previous_bookings_not_canceled = data.readInt();
		reserved_room_type = data.readInt();
		assigned_room_type = data.readUTF();
		booking_changes = data.readInt();
		deposit_type = data.readUTF();
		agent = data.readInt();
		company = data.readInt();
		days_in_waiting_list = data.readInt();
		customer_type = data.readUTF();
		adr = data.readDouble();
		required_car_parking_spaces = data.readInt();
		total_of_special_requests = data.readInt();
		reservation_status = data.readUTF();
		reservation_status_date = data.readUTF();
		
	}

	@Override
	public void write(DataOutput data) throws IOException {
		data.writeUTF(key);
		data.writeUTF(hotel);
		data.writeInt(is_canceled);
		data.writeInt(lead_timetime);
		data.writeInt(arrival_date_year);
		data.writeInt(arrival_date_month);
		data.writeInt(arrival_date_week_number);
		data.writeInt(arrival_date_day_of_month);
		data.writeInt(stays_in_weekend_nights);
		data.writeInt(stays_in_week_nights);
		data.writeInt(adults);
		data.writeInt(children);
		data.writeInt(babies);
		data.writeUTF(meal);
		data.writeUTF(country);
		data.writeUTF(market_segment);
		data.writeUTF(distribution_channel);
		data.writeInt(is_repeated_guest);
		data.writeInt(previous_cancellations); 
		data.writeInt(previous_bookings_not_canceled);
		data.writeInt(reserved_room_type);
		data.writeUTF(assigned_room_type);
		data.writeInt(booking_changes);
		data.writeUTF(deposit_type);
		data.writeInt(agent);
		data.writeInt(company);
		data.writeInt(days_in_waiting_list);
		data.writeUTF(customer_type);
		data.writeDouble(adr);
		data.writeInt(required_car_parking_spaces);
		data.writeInt(total_of_special_requests);
		data.writeUTF(reservation_status);
		data.writeUTF(reservation_status_date);
		
	}
	
	
}
