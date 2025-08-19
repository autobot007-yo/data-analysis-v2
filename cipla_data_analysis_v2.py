import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
import warnings
warnings.filterwarnings('ignore')

class SimplifiedAbandonCallsAnalyzer:
    def __init__(self):
        """
        SIMPLIFIED Business-Focused Abandon Calls Analysis
        Clean phone number tracking without complex case creation
        PRODUCTION-READY with clear business metrics
        """
        # Success criteria for disposition-based classification
        self.successful_dispositions = [
            'Others', 'MI', 'PQC', 'AE', 'AE MI', 'AE PQC', 'Non-case', 'Test', 'AE PQC MI', 'PQC MI', 'Inbound Follow-up', 'Translation AE'
        ]
        
        # Production tracking
        self.validation_errors = []
        self.data_quality_issues = []
        
    def log_validation_error(self, error_type, details):
        """Log validation errors for production monitoring"""
        self.validation_errors.append({
            'type': error_type,
            'details': details,
            'timestamp': datetime.now()
        })
        
    def log_data_quality_issue(self, issue_type, details):
        """Log data quality issues for production monitoring"""
        self.data_quality_issues.append({
            'type': issue_type,
            'details': details,
            'timestamp': datetime.now()
        })
    
    def validate_phone_number(self, phone_str):
        """Validate and normalize phone number"""
        if pd.isna(phone_str) or phone_str == '' or phone_str is None:
            return None
        try:
            digits_only = ''.join(filter(str.isdigit, str(phone_str)))
            if len(digits_only) < 10:
                self.log_data_quality_issue("Invalid Phone", f"Too short: {phone_str}")
                return None
            elif len(digits_only) > 15:
                self.log_data_quality_issue("Invalid Phone", f"Too long: {phone_str}")
                return None
            return digits_only[-10:]
        except Exception as e:
            self.log_data_quality_issue("Phone Processing Error", f"{phone_str}: {e}")
            return None
    
    def validate_timestamp(self, datetime_str):
        """Validate timestamp"""
        if pd.isna(datetime_str) or datetime_str is None or datetime_str == '':
            return None
        try:
            if isinstance(datetime_str, str) and ('AM' in datetime_str or 'PM' in datetime_str):
                parsed_date = pd.to_datetime(datetime_str, format='%m-%d-%Y %I:%M:%S %p')
            else:
                parsed_date = pd.to_datetime(datetime_str)
            
            # Validate reasonable date range
            min_date = datetime.now() - timedelta(days=730)
            max_date = datetime.now() + timedelta(days=365)
            
            if parsed_date < min_date or parsed_date > max_date:
                self.log_data_quality_issue("Invalid Date Range", f"Date outside reasonable range: {datetime_str}")
                return None
                
            return parsed_date
        except Exception as e:
            self.log_data_quality_issue("Date Parsing Error", f"{datetime_str}: {e}")
            return None
    
    def convert_to_business_date(self, datetime_obj):
        """Convert to business date using 6AM-6AM cycle"""
        if pd.isna(datetime_obj) or datetime_obj is None:
            return None
        try:
            if datetime_obj.hour < 6:
                business_date = datetime_obj.date() - timedelta(days=1)
            else:
                business_date = datetime_obj.date()
            return business_date
        except Exception as e:
            self.log_data_quality_issue("Business Date Error", f"{datetime_obj}: {e}")
            return None
    
    def time_to_seconds(self, time_str):
        """Convert HH:MM:SS to seconds"""
        if pd.isna(time_str) or time_str == "00:00:00" or time_str == "" or time_str is None:
            return 0
        try:
            parts = str(time_str).strip().split(':')
            if len(parts) == 3:
                hours = int(parts[0]) if parts[0].isdigit() else 0
                minutes = int(parts[1]) if parts[1].isdigit() else 0
                seconds = int(parts[2]) if parts[2].isdigit() else 0
                
                if hours > 24 or minutes > 59 or seconds > 59:
                    self.log_data_quality_issue("Invalid Time", f"Out of range: {time_str}")
                    return 0
                    
                return hours * 3600 + minutes * 60 + seconds
        except Exception as e:
            self.log_data_quality_issue("Time Conversion Error", f"{time_str}: {e}")
            return 0
        return 0
    
    def clean_dataframe_columns(self, df):
        """Clean DataFrame columns"""
        if df.empty:
            self.log_validation_error("Empty DataFrame", "No data to process")
            return df
            
        new_columns = []
        for col in df.columns:
            if pd.isna(col) or col is None:
                new_columns.append('')
            else:
                new_columns.append(str(col).strip())
        
        df.columns = new_columns
        print(f"📊 Processed DataFrame: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def safe_get_column(self, record, column_name, default=''):
        """Safely get column value"""
        try:
            if column_name in record.index:
                value = record[column_name]
                return value if not pd.isna(value) else default
            else:
                for col in record.index:
                    if isinstance(col, str) and column_name.lower() in col.lower():
                        value = record[col]
                        return value if not pd.isna(value) else default
                self.log_data_quality_issue("Missing Column", f"Column '{column_name}' not found")
                return default
        except Exception as e:
            self.log_data_quality_issue("Column Access Error", f"{column_name}: {e}")
            return default
    
    def extract_abandon_phone_numbers(self, acd_data):
        """
        SIMPLIFIED: Extract unique phone numbers that had abandon calls
        Returns: DataFrame with abandon phone details
        """
        acd_data = self.clean_dataframe_columns(acd_data)
        abandon_phone_data = {}  # phone -> abandon details
        invalid_records = 0
        
        print(f"📊 Processing {len(acd_data)} ACD records...")
        
        for idx, record in acd_data.iterrows():
            try:
                # Get and validate required fields
                phone_raw = self.safe_get_column(record, 'Phone', '')
                answered_hungup = self.safe_get_column(record, 'Answered/Hungup', '')
                wait_time_str = self.safe_get_column(record, 'Wait Time at ACD', '')
                call_time_raw = self.safe_get_column(record, 'Call Time', '')
                
                # Validate phone number
                normalized_phone = self.validate_phone_number(phone_raw)
                if not normalized_phone:
                    invalid_records += 1
                    continue
                
                # Validate timestamp
                call_datetime = self.validate_timestamp(call_time_raw)
                if not call_datetime:
                    invalid_records += 1
                    continue
                
                # Check if this is an abandon call
                wait_time_seconds = self.time_to_seconds(wait_time_str)
                if answered_hungup == 'HUNGUP' and wait_time_seconds > 27:
                    business_date = self.convert_to_business_date(call_datetime)
                    
                    # Track this abandon call for this phone number
                    if normalized_phone not in abandon_phone_data:
                        abandon_phone_data[normalized_phone] = {
                            'phone': normalized_phone,
                            'original_phone': phone_raw,
                            'first_abandon_time': call_datetime,
                            'first_abandon_time_str': call_time_raw,
                            'first_abandon_business_date': business_date,
                            'abandon_calls': [],
                            'total_abandon_calls': 0
                        }
                    
                    # Add this abandon call to the phone's record
                    abandon_phone_data[normalized_phone]['abandon_calls'].append({
                        'call_time': call_datetime,
                        'call_time_str': call_time_raw,
                        'business_date': business_date,
                        'wait_time_seconds': wait_time_seconds,
                        'wait_time_str': wait_time_str,
                        'queue_name': self.safe_get_column(record, 'Queue Name', ''),
                        'username': self.safe_get_column(record, 'Username', ''),
                        'disposition': self.safe_get_column(record, 'User Disposition Code', '')
                    })
                    
                    # Update total count
                    abandon_phone_data[normalized_phone]['total_abandon_calls'] += 1
                    
                    # Keep track of earliest abandon
                    if call_datetime < abandon_phone_data[normalized_phone]['first_abandon_time']:
                        abandon_phone_data[normalized_phone]['first_abandon_time'] = call_datetime
                        abandon_phone_data[normalized_phone]['first_abandon_time_str'] = call_time_raw
                        abandon_phone_data[normalized_phone]['first_abandon_business_date'] = business_date
                        
            except Exception as e:
                self.log_data_quality_issue("Record Processing Error", f"Row {idx}: {e}")
                invalid_records += 1
                continue
        
        print(f"✅ Found {len(abandon_phone_data)} unique phone numbers with abandon calls")
        print(f"📞 Total abandon calls: {sum(phone_data['total_abandon_calls'] for phone_data in abandon_phone_data.values())}")
        if invalid_records > 0:
            print(f"⚠️ Skipped {invalid_records} invalid records")
            
        return abandon_phone_data
    
    def find_recovery_calls(self, abandon_phone_data, acd_data, call_data):
        """
        SIMPLIFIED: Find recovery calls for abandon phone numbers
        Returns: Updated abandon_phone_data with recovery status
        """
        if not abandon_phone_data:
            self.log_validation_error("No Abandon Data", "No abandon phone numbers to search recovery for")
            return abandon_phone_data
            
        # Clean dataframes
        acd_data = self.clean_dataframe_columns(acd_data)
        call_data = self.clean_dataframe_columns(call_data) if call_data is not None and not call_data.empty else pd.DataFrame()
        
        recovery_found = 0
        
        for phone, phone_data in abandon_phone_data.items():
            try:
                first_abandon_time = phone_data['first_abandon_time']
                if not first_abandon_time:
                    continue
                
                # Search for recovery calls AFTER the first abandon (no strict 24-hour window)
                recovery_found_for_phone = False
                
                # Search inbound recovery (ACD data)
                for _, record in acd_data.iterrows():
                    try:
                        record_phone = self.validate_phone_number(self.safe_get_column(record, 'Phone', ''))
                        answered_hungup = self.safe_get_column(record, 'Answered/Hungup', '')
                        
                        if record_phone == phone and answered_hungup == 'ANSWERED':
                            call_time = self.validate_timestamp(self.safe_get_column(record, 'Call Time', ''))
                            
                            # Recovery call must be AFTER first abandon
                            if call_time and call_time > first_abandon_time:
                                disposition = self.safe_get_column(record, 'User Disposition Code', '')
                                
                                # Record recovery attempt
                                recovery_data = {
                                    'type': 'INBOUND',
                                    'time': call_time,
                                    'time_str': self.safe_get_column(record, 'Call Time', ''),
                                    'business_date': self.convert_to_business_date(call_time),
                                    'disposition': disposition,
                                    'username': self.safe_get_column(record, 'Username', ''),
                                    'talk_time': self.safe_get_column(record, 'User Talk Time', '00:00:00')
                                }
                                
                                # Check if successful
                                if disposition in self.successful_dispositions:
                                    phone_data['recovery_call'] = recovery_data
                                    phone_data['recovery_status'] = 'RECOVERED'
                                    recovery_found += 1
                                    recovery_found_for_phone = True
                                    break
                                elif 'recovery_call' not in phone_data:  # Record first attempt
                                    phone_data['recovery_call'] = recovery_data
                                    phone_data['recovery_status'] = 'ATTEMPTED'
                    except Exception:
                        continue
                
                # Search outbound recovery (CALL data) if no successful inbound found
                if not recovery_found_for_phone and not call_data.empty:
                    for _, record in call_data.iterrows():
                        try:
                            record_phone = self.validate_phone_number(self.safe_get_column(record, 'Phone', ''))
                            call_type = self.safe_get_column(record, 'Call Type', '')
                            system_disposition = self.safe_get_column(record, 'System Disposition', '')
                            
                            if (record_phone == phone and 
                                call_type == 'outbound.manual.dial' and
                                system_disposition == 'CONNECTED'):
                                
                                call_time = self.validate_timestamp(self.safe_get_column(record, 'Call Time', ''))
                                
                                # Recovery call must be AFTER first abandon
                                if call_time and call_time > first_abandon_time:
                                    disposition = self.safe_get_column(record, 'Disposition Code', '')
                                    
                                    # Record recovery attempt
                                    recovery_data = {
                                        'type': 'OUTBOUND',
                                        'time': call_time,
                                        'time_str': self.safe_get_column(record, 'Call Time', ''),
                                        'business_date': self.convert_to_business_date(call_time),
                                        'disposition': disposition,
                                        'username': self.safe_get_column(record, 'User Name', ''),
                                        'talk_time': self.safe_get_column(record, 'User Talk Time', '00:00:00')
                                    }
                                    
                                    # Check if successful
                                    if disposition in self.successful_dispositions:
                                        phone_data['recovery_call'] = recovery_data
                                        phone_data['recovery_status'] = 'RECOVERED'
                                        recovery_found += 1
                                        recovery_found_for_phone = True
                                        break
                                    elif 'recovery_call' not in phone_data:  # Record first attempt
                                        phone_data['recovery_call'] = recovery_data
                                        phone_data['recovery_status'] = 'ATTEMPTED'
                        except Exception:
                            continue
                
                # Set final status if no recovery found
                if not recovery_found_for_phone:
                    phone_data['recovery_status'] = 'NEEDS_OUTBOUND'
                    
            except Exception as e:
                self.log_data_quality_issue("Recovery Search Error", f"Phone {phone}: {e}")
                phone_data['recovery_status'] = 'NEEDS_OUTBOUND'
                continue
        
        print(f"🔍 Found recovery calls for {recovery_found} phone numbers")
        return abandon_phone_data
    
    def calculate_corrected_abandonment_rate(self, acd_data, abandon_phone_data):
        """
        SIMPLIFIED: Calculate abandonment rate
        Formula: ((total_abandon_calls - recovered_abandon_calls) / total_valid_calls) × 100
        """
        # Count total valid calls and abandon calls
        acd_data = self.clean_dataframe_columns(acd_data)
        valid_calls = 0
        total_abandon_calls = 0
        
        for _, record in acd_data.iterrows():
            try:
                phone_raw = self.safe_get_column(record, 'Phone', '')
                normalized_phone = self.validate_phone_number(phone_raw)
                
                if normalized_phone:  # Only count calls with valid phone numbers
                    valid_calls += 1
                    
                    # Check if this is an abandon call
                    answered_hungup = self.safe_get_column(record, 'Answered/Hungup', '')
                    wait_time_str = self.safe_get_column(record, 'Wait Time at ACD', '')
                    
                    if (answered_hungup == 'HUNGUP' and 
                        self.time_to_seconds(wait_time_str) > 27):
                        total_abandon_calls += 1
            except Exception:
                continue
        
        # Count recovered abandon calls
        recovered_abandon_calls = 0
        for phone, phone_data in abandon_phone_data.items():
            if phone_data.get('recovery_status') == 'RECOVERED':
                recovered_abandon_calls += phone_data['total_abandon_calls']
        
        # Calculate abandonment rate
        if valid_calls > 0:
            abandonment_rate = ((total_abandon_calls - recovered_abandon_calls) / valid_calls * 100)
            return round(abandonment_rate, 1)
        else:
            return 0
    
    def generate_summary_metrics(self, acd_data, abandon_phone_data):
        """
        SIMPLIFIED: Generate clean business metrics
        """
        acd_data = self.clean_dataframe_columns(acd_data)
        
        # Calculate metrics for valid calls only
        valid_calls = 0
        valid_answered = 0
        valid_hungup = 0
        quick_drops = 0
        total_abandon_calls = 0
        
        for _, record in acd_data.iterrows():
            try:
                phone_raw = self.safe_get_column(record, 'Phone', '')
                normalized_phone = self.validate_phone_number(phone_raw)
                
                if normalized_phone:  # Only count valid phone numbers
                    valid_calls += 1
                    
                    answered_hungup = self.safe_get_column(record, 'Answered/Hungup', '')
                    if answered_hungup == 'ANSWERED':
                        valid_answered += 1
                    elif answered_hungup == 'HUNGUP':
                        valid_hungup += 1
                        
                        wait_seconds = self.time_to_seconds(self.safe_get_column(record, 'Wait Time at ACD', ''))
                        if wait_seconds <= 27:
                            quick_drops += 1
                        else:
                            total_abandon_calls += 1
            except Exception:
                continue
        
        # Calculate phone-based metrics
        unique_abandon_phones = len(abandon_phone_data)
        recovered_phones = len([p for p in abandon_phone_data.values() if p.get('recovery_status') == 'RECOVERED'])
        phones_needing_calls = unique_abandon_phones - recovered_phones
        
        # Calculate abandonment rate
        abandonment_rate = self.calculate_corrected_abandonment_rate(acd_data, abandon_phone_data)
        
        return {
            'Total Valid Calls': valid_calls,
            'Total Answered Calls': valid_answered,
            'Total Hungup Calls': valid_hungup,
            'Quick Drops (≤27 sec)': quick_drops,
            'Abandon Calls (>27 sec)': total_abandon_calls,
            'Unique Abandon Phone Numbers': unique_abandon_phones,
            'Unique Phones Recovered': recovered_phones,
            'Unique Phones Needing Outbound Calls': phones_needing_calls,
            'Abandonment Rate (%)': abandonment_rate
        }
    
    def validate_final_metrics(self, metrics):
        """Validate final metrics for logical consistency"""
        validation_results = []
        
        try:
            total_calls = metrics['Total Valid Calls']
            answered = metrics['Total Answered Calls']
            hungup = metrics['Total Hungup Calls']
            quick_drops = metrics['Quick Drops (≤27 sec)']
            abandons = metrics['Abandon Calls (>27 sec)']
            unique_abandons = metrics['Unique Abandon Phone Numbers']
            recovered = metrics['Unique Phones Recovered']
            needing_calls = metrics['Unique Phones Needing Outbound Calls']
            
            # Validation 1: Total calls = Answered + Hungup
            if answered + hungup != total_calls:
                validation_results.append("❌ CRITICAL: Answered + Hungup ≠ Total Calls")
            else:
                validation_results.append("✅ Answered + Hungup = Total Calls")
            
            # Validation 2: Hungup = Quick Drops + Abandons
            if quick_drops + abandons != hungup:
                validation_results.append("❌ CRITICAL: Quick Drops + Abandons ≠ Total Hungup")
            else:
                validation_results.append("✅ Quick Drops + Abandons = Total Hungup")
            
            # Validation 3: Phone number math
            if recovered + needing_calls != unique_abandons:
                validation_results.append("❌ CRITICAL: Recovered + Needing Calls ≠ Unique Abandons")
            else:
                validation_results.append("✅ Recovered + Needing Calls = Unique Abandons")
            
            # Validation 4: Reasonable percentages
            abandon_rate = metrics['Abandonment Rate (%)']
            if abandon_rate < 0 or abandon_rate > 100:
                validation_results.append("❌ CRITICAL: Abandonment rate outside 0-100%")
            else:
                validation_results.append("✅ Abandonment rate within valid range")
                
        except Exception as e:
            validation_results.append(f"❌ CRITICAL: Validation error: {e}")
        
        return validation_results
    
    def get_unique_abandon_phones_for_date(self, acd_data, target_date):
        """Get unique abandon phone numbers for specific date"""
        if target_date is None:
            return 0
            
        unique_abandon_phones = set()
        
        for _, record in acd_data.iterrows():
            try:
                answered_hungup = self.safe_get_column(record, 'Answered/Hungup', '')
                wait_time_str = self.safe_get_column(record, 'Wait Time at ACD', '')
                call_time_str = self.safe_get_column(record, 'Call Time', '')
                
                if (answered_hungup == 'HUNGUP' and 
                    call_time_str and 
                    self.time_to_seconds(wait_time_str) > 27):
                    
                    call_datetime = self.validate_timestamp(call_time_str)
                    if call_datetime:
                        business_date = self.convert_to_business_date(call_datetime)
                        if business_date == target_date:
                            phone_raw = self.safe_get_column(record, 'Phone', '')
                            normalized_phone = self.validate_phone_number(phone_raw)
                            if normalized_phone:
                                unique_abandon_phones.add(normalized_phone)
            except Exception:
                continue
                
        return len(unique_abandon_phones)
    
    def create_daily_breakdown_report(self, acd_data, abandon_phone_data):
        """Generate simplified daily metrics"""
        acd_data = self.clean_dataframe_columns(acd_data)
        
        # Apply business day logic to valid data only
        valid_data = []
        for _, record in acd_data.iterrows():
            phone_raw = self.safe_get_column(record, 'Phone', '')
            if self.validate_phone_number(phone_raw):
                call_time_str = self.safe_get_column(record, 'Call Time', '')
                call_datetime = self.validate_timestamp(call_time_str)
                if call_datetime:
                    business_date = self.convert_to_business_date(call_datetime)
                    record_dict = record.to_dict()
                    record_dict['parsed_date'] = call_datetime
                    record_dict['business_date'] = business_date
                    valid_data.append(record_dict)
        
        if not valid_data:
            self.log_validation_error("No Valid Data", "No valid data for daily breakdown")
            return []
        
        valid_df = pd.DataFrame(valid_data)
        
        # Group abandon phones by business date
        phones_by_date = {}
        for phone, phone_data in abandon_phone_data.items():
            business_date = phone_data.get('first_abandon_business_date')
            if business_date:
                if business_date not in phones_by_date:
                    phones_by_date[business_date] = {'phones': [], 'recovered': 0}
                phones_by_date[business_date]['phones'].append(phone)
                if phone_data.get('recovery_status') == 'RECOVERED':
                    phones_by_date[business_date]['recovered'] += 1
        
        # Generate daily metrics
        daily_metrics = []
        daily_groups = valid_df.groupby('business_date')
        
        for business_date, day_data in daily_groups:
            if pd.isna(business_date):
                continue
                
            # Calculate daily metrics for valid data only
            total_calls = len(day_data)
            answered_calls = len([r for r in day_data.to_dict('records') 
                                if self.safe_get_column(pd.Series(r), 'Answered/Hungup', '') == 'ANSWERED'])
            hungup_calls = len([r for r in day_data.to_dict('records') 
                               if self.safe_get_column(pd.Series(r), 'Answered/Hungup', '') == 'HUNGUP'])
            
            # Calculate abandons and quick drops
            quick_drops = 0
            abandon_calls = 0
            
            for record_dict in day_data.to_dict('records'):
                record_series = pd.Series(record_dict)
                if self.safe_get_column(record_series, 'Answered/Hungup', '') == 'HUNGUP':
                    wait_seconds = self.time_to_seconds(self.safe_get_column(record_series, 'Wait Time at ACD', ''))
                    if wait_seconds <= 27:
                        quick_drops += 1
                    else:
                        abandon_calls += 1
            
            # Get phone metrics for this day
            day_phone_data = phones_by_date.get(business_date, {'phones': [], 'recovered': 0})
            unique_abandon_phones_today = len(day_phone_data['phones'])
            recovered_phones_today = day_phone_data['recovered']
            phones_needing_calls_today = unique_abandon_phones_today - recovered_phones_today
            
            # Calculate abandonment rate for this day
            recovered_abandon_calls_today = sum(
                abandon_phone_data[phone]['total_abandon_calls'] 
                for phone in day_phone_data['phones'] 
                if abandon_phone_data[phone].get('recovery_status') == 'RECOVERED'
            )
            abandonment_rate_today = ((abandon_calls - recovered_abandon_calls_today) / total_calls * 100) if total_calls > 0 else 0
            
            daily_metrics.append({
                'Business Date': business_date.strftime('%m-%d-%Y'),
                'Total Valid Calls': f"{total_calls} (100.0%)",
                'Total Answered Calls': f"{answered_calls} ({answered_calls/total_calls*100:.1f}%)" if total_calls > 0 else "0 (0.0%)",
                'Total Hungup Calls': f"{hungup_calls} ({hungup_calls/total_calls*100:.1f}%)" if total_calls > 0 else "0 (0.0%)",
                'Quick Drops (≤27 sec)': f"{quick_drops} ({quick_drops/total_calls*100:.1f}%)" if total_calls > 0 else "0 (0.0%)",
                'Abandon Calls (>27 sec)': f"{abandon_calls} ({abandon_calls/total_calls*100:.1f}%)" if total_calls > 0 else "0 (0.0%)",
                'Unique Abandon Phone Numbers': unique_abandon_phones_today,
                'Unique Phones Recovered': recovered_phones_today,
                'Unique Phones Needing Outbound Calls': phones_needing_calls_today,
                'Abandonment Rate (%)': f"{abandonment_rate_today:.1f}%"
            })
        
        # Sort by business date
        daily_metrics.sort(key=lambda x: datetime.strptime(x['Business Date'], '%m-%d-%Y'))
        
        return daily_metrics
    
    def create_excel_reports(self, acd_data, call_data, output_filename='simplified_abandon_analysis.xlsx'):
        """
        SIMPLIFIED: Generate clean business reports
        """
        print("🎯 Starting SIMPLIFIED Abandon Calls Analysis...")
        print("📋 Clean phone number tracking without complex case logic")
        
        try:
            # Execute simplified analysis
            abandon_phone_data = self.extract_abandon_phone_numbers(acd_data)
            abandon_phone_data = self.find_recovery_calls(abandon_phone_data, acd_data, call_data)
            
            # Generate clean summary metrics
            summary_metrics = self.generate_summary_metrics(acd_data, abandon_phone_data)
            
            # Validate metrics
            validation_results = self.validate_final_metrics(summary_metrics)
            print("\n📊 METRIC VALIDATION RESULTS:")
            for result in validation_results:
                print(f"   {result}")
            
            # Create Excel reports
            with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
                
                # Sheet 1: KPI Standard (Simplified)
                metrics_df = pd.DataFrame([
                    {'Metric': metric, 'Count': count, 'Percentage': 
                     f"{count/summary_metrics['Total Valid Calls']*100:.1f}%" if metric in ['Total Answered Calls', 'Total Hungup Calls', 'Quick Drops (≤27 sec)', 'Abandon Calls (>27 sec)'] 
                     else (f"{count}%" if metric == 'Abandonment Rate (%)' 
                     else "-")}
                    for metric, count in summary_metrics.items()
                ])
                metrics_df.to_excel(writer, sheet_name='KPI Standard', index=False)
                
                # Sheet 2: Initial Abandon Report (Phone Numbers)
                initial_abandon_data = []
                for phone, phone_data in abandon_phone_data.items():
                    initial_abandon_data.append({
                        'Phone Number': phone,
                        'Original Phone Format': phone_data['original_phone'],
                        'First Abandon Timestamp': phone_data['first_abandon_time_str'],
                        'First Abandon Business Date': phone_data['first_abandon_business_date'].strftime('%m-%d-%Y') if phone_data['first_abandon_business_date'] else 'N/A',
                        'Total Abandon Calls': phone_data['total_abandon_calls'],
                        'Status': 'Initial Abandon (Pre-Recovery Analysis)'
                    })
                
                initial_abandon_df = pd.DataFrame(initial_abandon_data)
                initial_abandon_df.to_excel(writer, sheet_name='Initial Abandon Report', index=False)
                
                # Sheet 3: Recovery Details (Phone Numbers)
                recovery_data = []
                for phone, phone_data in abandon_phone_data.items():
                    if phone_data.get('recovery_call'):
                        recovery_data.append({
                            'Phone Number': phone,
                            'First Abandon Timestamp': phone_data['first_abandon_time_str'],
                            'Recovery Call Timestamp': phone_data['recovery_call']['time_str'],
                            'Recovery Type': phone_data['recovery_call']['type'],
                            'Successful (Yes/No)': 'Yes' if phone_data.get('recovery_status') == 'RECOVERED' else 'No',
                            'User Name': phone_data['recovery_call']['username'],
                            'User Talk Time': phone_data['recovery_call'].get('talk_time', '00:00:00'),
                            'Disposition': phone_data['recovery_call']['disposition'],
                            'Recovery Status': phone_data.get('recovery_status', 'UNKNOWN')
                        })
                
                recovery_df = pd.DataFrame(recovery_data)
                recovery_df.to_excel(writer, sheet_name='Recovery Details', index=False)
                
                # Sheet 4: True Abandon Analysis (Final Status)
                true_abandon_data = []
                for phone, phone_data in abandon_phone_data.items():
                    true_abandon_data.append({
                        'Phone Number': phone,
                        'First Abandon Time': phone_data['first_abandon_time_str'],
                        'First Abandon Business Date': phone_data['first_abandon_business_date'].strftime('%m-%d-%Y') if phone_data['first_abandon_business_date'] else 'N/A',
                        'Total Abandon Calls': phone_data['total_abandon_calls'],
                        'Recovery Status': phone_data.get('recovery_status', 'NEEDS_OUTBOUND'),
                        'Final Status': 'Recovered' if phone_data.get('recovery_status') == 'RECOVERED' else 'Needs Outbound Call',
                        'Action Required': 'None - Recovered' if phone_data.get('recovery_status') == 'RECOVERED' else 'Schedule Outbound Call'
                    })
                
                true_abandon_df = pd.DataFrame(true_abandon_data)
                true_abandon_df.to_excel(writer, sheet_name='Final Phone Status', index=False)
                
                # Sheet 5: Team Leader Assignment (Clean)
                assignment_data = []
                for phone, phone_data in abandon_phone_data.items():
                    if phone_data.get('recovery_status') in ['NEEDS_OUTBOUND', 'ATTEMPTED']:
                        priority = 'High' if phone_data['total_abandon_calls'] > 2 else 'Medium' if phone_data['total_abandon_calls'] > 1 else 'Normal'
                        
                        assignment_data.append({
                            'Phone Number': phone,
                            'Priority Level': priority,
                            'Total Abandon Calls': phone_data['total_abandon_calls'],
                            'First Abandon Time': phone_data['first_abandon_time_str'],
                            'First Abandon Business Date': phone_data['first_abandon_business_date'].strftime('%m-%d-%Y') if phone_data['first_abandon_business_date'] else 'N/A',
                            'Recovery Status': phone_data.get('recovery_status', 'NEEDS_OUTBOUND'),
                            'Assignment Notes': f"Customer with {phone_data['total_abandon_calls']} abandon calls - Priority: {priority}"
                        })
                
                assignment_df = pd.DataFrame(assignment_data)
                if not assignment_df.empty:
                    assignment_df = assignment_df.sort_values(['Priority Level', 'Total Abandon Calls'], ascending=[True, False])
                assignment_df.to_excel(writer, sheet_name='Team Leader Assignment', index=False)
                
                # Sheet 6: Daily Row Format (Simplified)
                daily_metrics = self.create_daily_breakdown_report(acd_data, abandon_phone_data)
                daily_df = pd.DataFrame(daily_metrics)
                daily_df.to_excel(writer, sheet_name='Daily Row Format', index=False)
            
            print(f"✅ SIMPLIFIED Analysis complete! File: {output_filename}")
            
            # Print clean business summary
            print(f"\n📊 SIMPLIFIED BUSINESS SUMMARY:")
            print(f"   Total Abandon Calls: {summary_metrics['Abandon Calls (>27 sec)']}")
            print(f"   Unique Phone Numbers with Abandons: {summary_metrics['Unique Abandon Phone Numbers']}")
            print(f"   Unique Phone Numbers Recovered: {summary_metrics['Unique Phones Recovered']}")
            print(f"   Unique Phone Numbers Needing Outbound Calls: {summary_metrics['Unique Phones Needing Outbound Calls']}")
            print(f"   Abandonment Rate: {summary_metrics['Abandonment Rate (%)']}%")
            print(f"   ✅ Simple Math: {summary_metrics['Unique Abandon Phone Numbers']} - {summary_metrics['Unique Phones Recovered']} = {summary_metrics['Unique Phones Needing Outbound Calls']}")
            
            # Report data quality
            if self.data_quality_issues:
                print(f"\n⚠️ DATA QUALITY ISSUES: {len(self.data_quality_issues)}")
                issue_summary = {}
                for issue in self.data_quality_issues:
                    issue_type = issue['type']
                    issue_summary[issue_type] = issue_summary.get(issue_type, 0) + 1
                
                for issue_type, count in issue_summary.items():
                    print(f"   {issue_type}: {count} instances")
            
            return abandon_phone_data, summary_metrics
            
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
            self.log_validation_error("Critical Error", str(e))
            return {}, {}

def main():
    """
    SIMPLIFIED Main execution
    """
    analyzer = SimplifiedAbandonCallsAnalyzer()
    
    print("📂 Loading data files...")
    
    # File paths
    acd_file = "ACD__18 Aug 25.xlsx"
    call_file = "CALL_Details_18 Aug 25.xlsx"
    
    try:
        # Load ACD data
        print(f"📥 Loading: {acd_file}")
        acd_data = pd.read_excel(acd_file)
        print(f"✅ Loaded {len(acd_data)} ACD records")
        
        # Load CALL data
        call_data = pd.DataFrame()
        try:
            print(f"📥 Loading: {call_file}")
            call_data = pd.read_excel(call_file)
            print(f"✅ Loaded {len(call_data)} CALL records")
        except FileNotFoundError:
            print(f"⚠️ CALL data file not found - proceeding with ACD data only")
        
        # Run simplified analysis
        abandon_phone_data, metrics = analyzer.create_excel_reports(acd_data, call_data)
        
        print("\n🎯 SIMPLIFIED BUSINESS ANALYSIS COMPLETE!")
        
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")

if __name__ == "__main__":
    main()