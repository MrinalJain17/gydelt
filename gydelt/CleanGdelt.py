import pandas as pd
from timeit import default_timer as timer

countries = [
	('US', 'United States'),
	('AF', 'Afghanistan'),
	('AL', 'Albania'),
	('DZ', 'Algeria'),
	('AS', 'American Samoa'),
	('AD', 'Andorra'),
	('AO', 'Angola'),
	('AI', 'Anguilla'),
	('AQ', 'Antarctica'),
	('AG', 'Antigua And Barbuda'),
	('AR', 'Argentina'),
	('AM', 'Armenia'),
	('AW', 'Aruba'),
	('AU', 'Australia'),
	('AT', 'Austria'),
	('AZ', 'Azerbaijan'),
	('BS', 'Bahamas'),
	('BH', 'Bahrain'),
	('BD', 'Bangladesh'),
	('BB', 'Barbados'),
	('BY', 'Belarus'),
	('BE', 'Belgium'),
	('BZ', 'Belize'),
	('BJ', 'Benin'),
	('BM', 'Bermuda'),
	('BT', 'Bhutan'),
	('BO', 'Bolivia'),
	('BA', 'Bosnia And Herzegowina'),
	('BW', 'Botswana'),
	('BV', 'Bouvet Island'),
	('BR', 'Brazil'),
	('BN', 'Brunei Darussalam'),
	('BG', 'Bulgaria'),
	('BF', 'Burkina Faso'),
	('BI', 'Burundi'),
	('KH', 'Cambodia'),
	('CM', 'Cameroon'),
	('CA', 'Canada'),
	('CV', 'Cape Verde'),
	('KY', 'Cayman Islands'),
	('CF', 'Central African Rep'),
	('TD', 'Chad'),
	('CL', 'Chile'),
	('CN', 'China'),
	('CX', 'Christmas Island'),
	('CC', 'Cocos Islands'),
	('CO', 'Colombia'),
	('KM', 'Comoros'),
	('CG', 'Congo'),
	('CK', 'Cook Islands'),
	('CR', 'Costa Rica'),
	('CI', 'Cote D`ivoire'),
	('HR', 'Croatia'),
	('CU', 'Cuba'),
	('CY', 'Cyprus'),
	('CZ', 'Czech Republic'),
	('DK', 'Denmark'),
	('DJ', 'Djibouti'),
	('DM', 'Dominica'),
	('DO', 'Dominican Republic'),
	('TP', 'East Timor'),
	('EC', 'Ecuador'),
	('EG', 'Egypt'),
	('SV', 'El Salvador'),
	('GQ', 'Equatorial Guinea'),
	('ER', 'Eritrea'),
	('EE', 'Estonia'),
	('ET', 'Ethiopia'),
	('FK', 'Falkland Islands (Malvinas)'),
	('FO', 'Faroe Islands'),
	('FJ', 'Fiji'),
	('FI', 'Finland'),
	('FR', 'France'),
	('GF', 'French Guiana'),
	('PF', 'French Polynesia'),
	('TF', 'French S. Territories'),
	('GA', 'Gabon'),
	('GM', 'Gambia'),
	('GE', 'Georgia'),
	('DE', 'Germany'),
	('GH', 'Ghana'),
	('GI', 'Gibraltar'),
	('GR', 'Greece'),
	('GL', 'Greenland'),
	('GD', 'Grenada'),
	('GP', 'Guadeloupe'),
	('GU', 'Guam'),
	('GT', 'Guatemala'),
	('GN', 'Guinea'),
	('GW', 'Guinea-bissau'),
	('GY', 'Guyana'),
	('HT', 'Haiti'),
	('HN', 'Honduras'),
	('HK', 'Hong Kong'),
	('HU', 'Hungary'),
	('IS', 'Iceland'),
	('IN', 'India'),
	('ID', 'Indonesia'),
	('IR', 'Iran'),
	('IQ', 'Iraq'),
	('EI', 'Ireland'),
	('IL', 'Israel'),
	('IT', 'Italy'),
	('JM', 'Jamaica'),
	('JP', 'Japan'),
	('JO', 'Jordan'),
	('KZ', 'Kazakhstan'),
	('KE', 'Kenya'),
	('KI', 'Kiribati'),
	('KP', 'North Korea'),
	('KR', 'South Korea'),
	('KW', 'Kuwait'),
	('KG', 'Kyrgyzstan'),
	('LA', 'Laos'),
	('LV', 'Latvia'),
	('LB', 'Lebanon'),
	('LS', 'Lesotho'),
	('LR', 'Liberia'),
	('LY', 'Libya'),
	('LI', 'Liechtenstein'),
	('LT', 'Lithuania'),
	('LU', 'Luxembourg'),
	('MO', 'Macau'),
	('MK', 'Macedonia'),
	('MG', 'Madagascar'),
	('MW', 'Malawi'),
	('MY', 'Malaysia'),
	('MV', 'Maldives'),
	('ML', 'Mali'),
	('MT', 'Malta'),
	('MH', 'Marshall Islands'),
	('MQ', 'Martinique'),
	('MR', 'Mauritania'),
	('MU', 'Mauritius'),
	('YT', 'Mayotte'),
	('MX', 'Mexico'),
	('FM', 'Micronesia'),
	('MD', 'Moldova'),
	('MC', 'Monaco'),
	('MN', 'Mongolia'),
	('MS', 'Montserrat'),
	('MA', 'Morocco'),
	('MZ', 'Mozambique'),
	('MM', 'Myanmar'),
	('NA', 'Namibia'),
	('NR', 'Nauru'),
	('NP', 'Nepal'),
	('NL', 'Netherlands'),
	('AN', 'Netherlands Antilles'),
	('NC', 'New Caledonia'),
	('NZ', 'New Zealand'),
	('NI', 'Nicaragua'),
	('NE', 'Niger'),
	('NG', 'Nigeria'),
	('NU', 'Niue'),
	('NF', 'Norfolk Island'),
	('MP', 'Northern Mariana Islands'),
	('NO', 'Norway'),
	('OM', 'Oman'),
	('PK', 'Pakistan'),
	('PW', 'Palau'),
	('PA', 'Panama'),
	('PG', 'Papua New Guinea'),
	('PY', 'Paraguay'),
	('PE', 'Peru'),
	('PH', 'Philippines'),
	('PN', 'Pitcairn'),
	('PL', 'Poland'),
	('PT', 'Portugal'),
	('PR', 'Puerto Rico'),
	('QA', 'Qatar'),
	('RE', 'Reunion'),
	('RO', 'Romania'),
	('RS', 'Russia'),
	('RW', 'Rwanda'),
	('KN', 'Saint Kitts And Nevis'),
	('LC', 'Saint Lucia'),
	('VC', 'St Vincent/Grenadines'),
	('WS', 'Samoa'),
	('SM', 'San Marino'),
	('ST', 'Sao Tome'),
	('SA', 'Saudi Arabia'),
	('SN', 'Senegal'),
	('SC', 'Seychelles'),
	('SL', 'Sierra Leone'),
	('SG', 'Singapore'),
	('SK', 'Slovakia'),
	('SI', 'Slovenia'),
	('SB', 'Solomon Islands'),
	('SO', 'Somalia'),
	('ZA', 'South Africa'),
	('ES', 'Spain'),
	('LK', 'Sri Lanka'),
	('SH', 'St. Helena'),
	('PM', 'St.Pierre'),
	('SD', 'Sudan'),
	('SR', 'Suriname'),
	('SZ', 'Swaziland'),
	('SE', 'Sweden'),
	('CH', 'Switzerland'),
	('SY', 'Syria'),
	('TW', 'Taiwan'),
	('TJ', 'Tajikistan'),
	('TZ', 'Tanzania'),
	('TH', 'Thailand'),
	('TG', 'Togo'),
	('TK', 'Tokelau'),
	('TO', 'Tonga'),
	('TT', 'Trinidad And Tobago'),
	('TN', 'Tunisia'),
	('TR', 'Turkey'),
	('TM', 'Turkmenistan'),
	('TV', 'Tuvalu'),
	('UG', 'Uganda'),
	('UA', 'Ukraine'),
	('AE', 'United Arab Emirates'),
	('UK', 'United Kingdom'),
	('UY', 'Uruguay'),
	('UZ', 'Uzbekistan'),
	('VU', 'Vanuatu'),
	('VA', 'Vatican City State'),
	('VE', 'Venezuela'),
	('VN', 'Vietnam'),
	('VG', 'Virgin Islands (British)'),
	('VI', 'Virgin Islands (U.S.)'),
	('EH', 'Western Sahara'),
	('YE', 'Yemen'),
	('YU', 'Yugoslavia'),
	('ZR', 'Zaire'),
	('ZM', 'Zambia'),
	('ZW', 'Zimbabwe')
]

US_states = [
	('AK', 'Alaska'),
	('AL', 'Alabama'),
	('AR', 'Arkansas'),
	('AS', 'American Samoa'),
	('AZ', 'Arizona'),
	('CA', 'California'),
	('CO', 'Colorado'),
	('CT', 'Connecticut'),
	('DC', 'District of Columbia'),
	('DE', 'Delaware'),
	('FL', 'Florida'),
	('GA', 'Georgia'),
	('GU', 'Guam'),
	('HI', 'Hawaii'),
	('IA', 'Iowa'),
	('ID', 'Idaho'),
	('IL', 'Illinois'),
	('IN', 'Indiana'),
	('KS', 'Kansas'),
	('KY', 'Kentucky'),
	('LA', 'Louisiana'),
	('MA', 'Massachusetts'),
	('MD', 'Maryland'),
	('ME', 'Maine'),
	('MI', 'Michigan'),
	('MN', 'Minnesota'),
	('MO', 'Missouri'),
	('MP', 'Northern Mariana Islands'),
	('MS', 'Mississippi'),
	('MT', 'Montana'),
	('NA', 'National'),
	('NC', 'North Carolina'),
	('ND', 'North Dakota'),
	('NE', 'Nebraska'),
	('NH', 'New Hampshire'),
	('NJ', 'New Jersey'),
	('NM', 'New Mexico'),
	('NV', 'Nevada'),
	('NY', 'New York'),
	('OH', 'Ohio'),
	('OK', 'Oklahoma'),
	('OR', 'Oregon'),
	('PA', 'Pennsylvania'),
	('PR', 'Puerto Rico'),
	('RI', 'Rhode Island'),
	('SC', 'South Carolina'),
	('SD', 'South Dakota'),
	('TN', 'Tennessee'),
	('TX', 'Texas'),
	('UT', 'Utah'),
	('VA', 'Virginia'),
	('VI', 'Virgin Islands'),
	('VT', 'Vermont'),
	('WA', 'Washington'),
	('WI', 'Wisconsin'),
	('WV', 'West Virginia'),
	('WY', 'Wyoming')
]

def check_country_list(data_frame, column_name='Locations'):

	"""Returns those Locations (countries) which were not present in the countries list
	
	Args:
		data_frame (pandas.DataFrame) : 
			The DataFrame in which the column 'Locations' needs to be cross-checked with the default country list
		column_name (str) : default 'Locations'
			The name of the column in the passed DataFrame that is to be checked with the default country list
			If not 'Locations', pass this parameter with the correct column name EXPLICITLY

	Returns:
		list
			A list containing the locations for which there was no match in the default country list
			
			-> User can then modify the country list accordingly
		
	"""

	missing = []
	loc = ''
	for locations in data_frame[column_name]:
		count = 0
		location = locations.split(';')
		for c in countries:
			for loc in location:
				if (c[1] in loc):
					count = 1
		if (count == 0):
			missing.append(loc)
	return missing


def clean_locations(data_frame, only_country=True, fillna='unknown', column_name='Locations'):

	"""Pre-process the 'Locations' column of the data (Extract either all details available, or just the Countries)
	
	Args:
		data_frame (pandas.DataFrame) :
			The DataFrame in which the column 'Locations' has to be cleaned/filtered
		only_country (boolean) : default True
			If True, will keep only the Country names for each row in the 'Locations' column
			If False, will keep whatever details available (city, state or country)
		fillna (str) : default 'unknown'
			To fill the Null values (NaN) with the specified value
		column_name (str) : default 'Locations'
			The name of the column in the passed DataFrame that is to be cleaned
			If not 'Locations', pass this parameter with the correct column name EXPLICITLY
	
	Returns:
		pandas.DataFrame
			A pandas DataFrame (with additional columns for 'Countries' and 'States', if required)
		
	"""

	data_frame[column_name].fillna(fillna, inplace=True)
	if 'Countries' not in list(data_frame.columns):
		data_frame.loc[:,'Countries'] = data_frame[column_name].apply(lambda x: process_locations(row=x))
	if only_country:
		pass
	else:
		data_frame.loc[:,'States'] = data_frame[column_name].apply(lambda x: process_locations_states(row=x))
	return data_frame


def clean_persons(data_frame, fillna='unknown', column_name='Persons', max_no_of_words=6):

	"""Filters out the 'Persons' column of the data.
	
	Only those names are kept in which the no. of words are within a certain limit
	
	Args:
		data_frame (pandas.DataFrame) :
			The DataFrame in which the column 'Persons' has to be cleaned
		fillna (str) : default 'unknown'
			To fill the Null values (NaN) with the specified value
		column_name (str) : default 'Persons'
			The name of the column in the passed DataFrame that is to be cleaned
			If not 'Persons', pass this parameter with the correct column name EXPLICITLY
		max_no_of_words (int) : default 6
			Removes all the names whose length is greater than this value from each record/row
		
	Returns:
		pandas.DataFrame
			A pandas DataFrame (with updated 'Persons')
		
	"""

	data_frame[column_name].fillna(fillna, inplace=True)
	data_frame.loc[:,column_name] = data_frame[column_name].apply(lambda x: process_persons(x, max_words=max_no_of_words))
	return data_frame


def clean_organizations(data_frame, fillna='unknown', column_name='Organizations'):

	"""Pre-processes the 'Organizations' column. Removes certain invalid Organizations
	
	Note:
		Some Countries (eg. United States) have been mistaken as individual Organizations
		This function removes those Organizations (which are actually Countries), from each record/row
	
	Args:
		data_frame (pandas.DataFrame) :
			The DataFrame in which the column 'Organizations' has to be cleaned
		fillna (str) : default 'unknown'
			To fill the Null values (NaN) with the specified value
		column_name (str) : default 'Organizations'
			The name of the column in the passed DataFrame that is to be cleaned
			If not 'Organizations', pass this parameter with the correct column name EXPLICITLY
		
	Returns:
		pandas.DataFrame
			A pandas DataFrame (with updated 'Organizations')
	
	"""

	data_frame[column_name].fillna(fillna, inplace=True)
	data_frame.loc[:,column_name] = data_frame[column_name].apply(lambda x: process_organizations(x))
	data_frame[column_name].fillna(fillna, inplace=True)      # If after processing the Organizations, a NaN value is created
	return data_frame


def seperate_tones(data_frame, column_name='ToneData'):
	
	"""Creates Seperate columns for each value in 'ToneData'
	
	Args:
		data_frame (pandas.DataFrame) :
			The DataFrame in which the column 'ToneData' has to be segregated/seperated
		column_name (str) : default 'ToneData'
			The name of the column in the passed DataFrame that is to be cleaned/segregated
			If not 'ToneData', pass this parameter with the correct column name EXPLICITLY
	
	Returns:
		pandas.DataFrame
			A pandas DataFrame
			
			-> The 'ToneData' column has 7 vaules, which are converted into seperate columns in the data frame
			-> The original 'ToneData' remains intact
		
	"""
	
	seperate = ['Tone', 'Positive Score', 'Negative Score', 'Polarity', 'Activity Reference Density', 'Self/Group Reference Density', 'Word Count']
	for i in range(len(seperate)):
		try:
			data_frame.loc[:,seperate[i]] = data_frame[column_name].apply(lambda x: float('{:.15f}'.format(float(x.split(',')[i]))))
		except IndexError:
			data_frame.loc[:,seperate[i]] = None
	return data_frame
	
	
def clean_themes(data_frame, fillna='unknown', column_name='Themes'):
	
	"""Fills the Null values (NaN) in the 'Themes' column of data
	
	Args: 
		data_frame (pandas.DataFrame) :
			The DataFrame in which the the Null values (NaN) are to be filled in the 'Themes' column
		fillna (str) : default 'unknown'
			To fill the Null values (NaN) with the specified value
		column_name (str) : default 'Themes'
			The name of the column in the passed DataFrame that is to be cleaned
			If not 'Themes', pass this parameter with the correct column name EXPLICITLY
		
	Returns:
		pandas.DataFrame
			A pandas DataFrame (with Null values in 'Themes' filled)
		
	"""
	
	data_frame[column_name].fillna(fillna, inplace=True)
	return data_frame

	
def flat_column(data_frame, columns=[], fillna='unknown'):

	"""The given list of columns are flattened (using one-hot encoding) and the result is added to the DataFrame
	
	Args:
		data_frame (pandas.DataFrame) :
			The DataFrame from which the 'column' has to be flattened
		fillna (str) : default 'unknown'
			To fill the Null values (NaN) with the specified value
		
	Returns:
		pandas.DataFrame
			A pandas DataFrame
			
			-> All the colums passed in the list 'columns' are flattened (one-hot encoding is used)
			-> The new data frame returned contains additional columns, which are the individual and unique values present in the respective columns which are required to be flattened	
			-> Also prints the time taken to flatten the columns
		
	"""
	
	start_time = timer()
	
	if len(columns) == 0:
		print('No columns passed to flatten !')
		return data_frame
	else:
		for column in columns:
			data_frame[column].fillna(fillna, inplace=True)
			values = []
			for i in data_frame[column]:
				values.extend(i.split(';'))

			values = list(set(values))
			if '' in values:
				values.remove('')
			values.sort()
			
			for i in values:
				data_frame.loc[:,i] = data_frame[column].apply(lambda x: one_hot_encode(x, i))
			
		end_time = timer()
		print('\nTime taken for flattening the column(s) --> {:.2f} seconds'.format(end_time - start_time))
		
		return data_frame
	
	
def pre_process(data_frame):
	
	"""A wrapper functions that does ALL the pre-processig. (Flattening is not performed)
	
	Args: 
		data_frame (pandas.DataFrame) :
			The DataFrame which is to be pre-processed
		
	Returns:
		pandas.DataFrame
			A clean and processed pandas DataFrame
			
			-> Also prints the time taken to pre-process the data
		
	"""
	
	start_time = timer()
	
	to_clean = list(data_frame.columns)
	if 'Locations' in to_clean:
		data_frame = clean_locations(data_frame=data_frame, only_country=False)
	if 'Persons' in to_clean:
		data_frame = clean_persons(data_frame=data_frame)
	if 'Organizations' in to_clean:
		data_frame = clean_organizations(data_frame=data_frame)
	if 'ToneData' in to_clean:
		data_frame = seperate_tones(data_frame=data_frame)
	if 'Themes' in to_clean:
		data_frame = clean_themes(data_frame=data_frame)
		
	end_time = timer()
	print('\nTime taken for pre-processing the data --> {:.2f} seconds'.format(end_time - start_time))
	
	return data_frame



def process_persons(row, max_words):

	"""Returns only those names having a no. of words below a certain limit
	
	Note:
		Used Internally
		
	"""
	
	names = row.split(';')
	finalNames = []
	for name in names:
		if (len(name.split(' ')) <= max_words):
			finalNames.append(name)
	return ';'.join(finalNames)


def process_locations(row):

	"""Returns the Names of Countries from each row (delimited by semi-colon)
	
	Note:
		Used Internally
	
	"""

	temp = row.split(';')
	cleanList = []
	for c in countries:
		for t in temp:
			if (c[1] in t):
				cleanList.append(c[1])               # This will only extract the Country name
	return ';'.join(set(cleanList))
	
	
def process_locations_states(row):

	"""Returns the States (of the US) from each row, if present (delimited by semi-colon)
	
	Note:
		Used Internally
	
	"""
	
	temp = row.split(';')
	stateList = []
	for t in temp:
		if 'United States' in t:
			for state in US_states:
				if state[1] in t:
					stateList.append(state[1])
	if len(stateList) == 0:
		return 'unknown'
	return ';'.join(set(stateList))


def process_organizations(row):

	"""Removes those organizations which have the exact same name as a country
	
	Note:
		Used Internally
	
	"""

	organizations = row.split(';')
	organizations = [org.lower() for org in organizations]
	for country in countries:
		if country[1].lower() in organizations:
			organizations.remove(country[1].lower())
	return ';'.join(organizations)


def one_hot_encode(row, val):

	""" Used for One-Hot Encoding
	
	Checks if the value passed is present in the current row
	If Yes, then the value of that particular column, for that row is 1
	
	Note:
		Used Internally
		
	"""

	if val in row.split(';'):
		return 1