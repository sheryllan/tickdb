# -*- coding: utf-8 -*-
"""
Created on Mon Dec  3 11:16:23 2018

@author: mshapiro
"""

import pytz
from datetime import datetime
from datetime import timedelta
from numpy import int64, signedinteger, unsignedinteger


class Nanoseconds:
    """A nanosecond class with some helpful conversions"""
    def __init__(self, nanos):
        self.__nanos = nanos
            
    @property
    def nanoseconds(self):
        return self.__nanos
    
    @property
    def microseconds(self):
        return self.nanoseconds/1000
    
    @property
    def milliseconds(self):
        return self.microseconds/1000    
        
    @property
    def seconds(self):
        return self.milliseconds/1000
    
    @property
    def minutes(self):
        return self.seconds/60
    
    @property
    def hours(self):
        return self.minutes/60

    def __str__(self):
        return str(self.__nanos)
    
    def __lt__(self, other):
        if isinstance(other, Nanoseconds):
            return self.__nanos < other.__nanos
        else:
            raise TypeError("Cannot handle this type: ", type(other))
            
    def __gt__(self, other):
        if isinstance(other, Nanoseconds):
            return self.__nanos > other.__nanos
        else:
            raise TypeError("Cannot handle this type: ", type(other))
            
    def __eq__(self, other):
        if isinstance(other, Nanoseconds):
            return self.__nanos == other.__nanos
        else:
            raise TypeError("Cannot handle this type: ", type(other))
            
    def __le__(self, other):
        if isinstance(other, Nanoseconds):
            return self.__nanos <= other.__nanos
        else:
            raise TypeError("Cannot handle this type: ", type(other))
            
    def __ge__(self, other):
        if isinstance(other, Nanoseconds):
            return self.__nanos >= other.__nanos
        else:
            raise TypeError("Cannot handle this type: ", type(other))
        
    def __add__(self, other):
        if isinstance(other, Nanoseconds):
            return Nanoseconds(self.__nanos + other.__nanos)    
        elif isinstance(other, (int, signedinteger, unsignedinteger)):
            return Nanoseconds(self.__nanos + other)  
        else:
            raise TypeError("Cannot handle this type: ", type(other))
    
    def __sub__(self, other):
        if isinstance(other, Nanoseconds):
            return Nanoseconds(self.__nanos - other.__nanos)
        elif isinstance(other, (int, signedinteger, unsignedinteger)):
            return Nanoseconds(self.__nanos - other)  
        else:
            raise TypeError("Cannot handle this type: ", type(other))
            
    def __hash__(self):
        return self.__nanos
    

class NanoTime:
    """

    The NanoTime class helps to allow handling of time that is nanosecond accurate.
    You can subtract and compare NanoTimes as well as print them in a pretty way.
    """
    def __init__(self, utc_nanoseconds_since_epoch=None, dt=None, tzinfo=None):
        self.__intialized = False
        self.__utc_value = 0
        self.__UTC_Time = None
        self.__nanos = None
        self.__tzinfo = None
        self.__timezone = pytz.utc
        self.__epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
        if utc_nanoseconds_since_epoch is not None or dt is not None:
            self.initialize(utc_nanoseconds_since_epoch, dt, tzinfo)
            
    def __clear(self):
        self.__intialized = False
        self.__utc_value = 0
        self.__UTC_Time = None
        self.__nanos = None
        self.__tzinfo = None
        self.__timezone = pytz.utc
        
    @property
    def nanoseconds(self):
        return int64(self.__utc_value)
    
    def initialize(self, utc_nanoseconds_since_epoch=None, dt=None, tzinfo=None):
        """
        :param utc_nanoseconds_since_epoch: Nanoseconds since epoch in UTC
        :param dt: The datetime object we want to read in
        :param tzinfo: The timezone we want to display into

        Initializes the NanoTime class with an integer
        which is the utc_nanoseconds_since_epoch or from a datetime (dt).
        
        dt is assumed to be UTC if not supplied with a tzinfo.
        
        One can also supply the default timezone 
         you would like the time values printed in with tzinfo
         e.g 'Australia/Sydney' or 'Europe/Germany'.
        """
        if not self.__intialized:
            try:
                if tzinfo is not None:
                    self.__timezone = pytz.timezone(tzinfo)
                    self.__tzinfo = tzinfo
                else:
                    self.__timezone = pytz.utc
                if isinstance(utc_nanoseconds_since_epoch, (int, signedinteger, unsignedinteger)) and \
                        utc_nanoseconds_since_epoch >= 1000:
                    self.__intialized = True
                    self.__utc_value = int(utc_nanoseconds_since_epoch)
                    self.__nanos = int(str(utc_nanoseconds_since_epoch)[-3:])
                    microseconds_since_epoch = int(str(utc_nanoseconds_since_epoch)[:-3])
                    seconds_since_epoch = microseconds_since_epoch/1000/1000
                    self.__UTC_Time = self.__epoch+timedelta(0, seconds_since_epoch)
                elif isinstance(dt, datetime):
                    self.__intialized = True
                    self.__nanos = 0
                    if dt.tzinfo is None:
                        seconds = (datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                                            dt.second, tzinfo=pytz.utc)
                                   - self.__epoch).total_seconds()
                        self.__utc_value = int(seconds*1000*1000*1000+dt.microsecond * 1000)
                        self.__UTC_Time = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                                                   dt.second, dt.microsecond, tzinfo=pytz.utc)
                    else:
                        utc_date = dt.astimezone(pytz.utc)
                        seconds = (datetime(utc_date.year, utc_date.month, utc_date.day,
                                            utc_date.hour, utc_date.minute, utc_date.second,
                                            tzinfo=pytz.utc)-self.__epoch).total_seconds()
                        self.__utc_value = int(seconds*1000*1000*1000+utc_date.microsecond * 1000)
                        self.__UTC_Time = datetime(utc_date.year, utc_date.month, utc_date.day,
                                                   utc_date.hour, utc_date.minute, utc_date.second,
                                                   utc_date.microsecond, tzinfo=pytz.utc)
                else:
                    raise ValueError("Cannot Initialize: " + str(utc_nanoseconds_since_epoch) + " of type " +
                                     str(type(utc_nanoseconds_since_epoch)))
            except Exception as e:
                self.__clear()
                raise e
        else:
            raise ValueError("Cannot Initialize as we are already initialized")

    def __str__(self):
        if self.__UTC_Time is None:
            return ""
        else:
            tz_date = self.__UTC_Time.astimezone(self.__timezone)
            if self.__timezone != pytz.utc:
                tz_info = tz_date.strftime("%z")[:-2] + ":"+tz_date.strftime("%z")[-2:]
            else:
                tz_info = ""
            micro_str = tz_date.strftime("%f")
            nano_str = '{:03d}'.format(self.__nanos)
            return tz_date.strftime("%Y-%m-%dT%H:%M:%S.") + micro_str+nano_str+tz_info
        
    def __checkinit(self):
        if not self.__intialized:
            raise ValueError("Not Initialized")
    
    def __hash__(self):
        return self.__utc_value
    
    def __lt__(self, other):
        self.__checkinit()
        if isinstance(other, NanoTime):
            other.__checkinit()
            return self.__utc_value < other.__utc_value
        else:
            raise TypeError("Cannot handle this type: ", type(other))
    
    def __gt__(self, other):
        self.__checkinit()
        if isinstance(other, NanoTime):
            other.__checkinit()
            return self.__utc_value > other.__utc_value
        else:
            raise TypeError("Cannot handle this type: ", type(other))
    
    def __eq__(self, other):
        self.__checkinit()
        if isinstance(other, NanoTime):
            other.__checkinit()
            return self.__utc_value == other.__utc_value
        elif isinstance(other, (int, unsignedinteger, signedinteger)):
            return other == self.__utc_value
        elif isinstance(other, Nanoseconds):
            return other.nanoseconds == self.__utc_value
        else:
            return False
    
    def __ge__(self, other):
        self.__checkinit()
        if isinstance(other, NanoTime):
            other.__checkinit()
            return self.__utc_value >= other.__utc_value
        else:
            raise TypeError("Cannot handle this type: ", type(other))
    
    def __le__(self, other):
        self.__checkinit()
        if isinstance(other, NanoTime):
            other.__checkinit()
            return self.__utc_value <= other.__utc_value
        else:
            raise TypeError("Cannot handle this type: ", type(other))
    
    def __len__(self):
        if self.__UTC_Time is None:
            return 0
        else:
            return 1
        
    def __sub__(self, other):
        """
        Return the number of nanoseconds between the two results
        """
        self.__checkinit()
        if isinstance(other, NanoTime):
            other.__checkinit()
            return Nanoseconds(self.__utc_value - other.__utc_value)
        elif isinstance(other, Nanoseconds):
            return NanoTime(self.__utc_value - other.nanoseconds, tzinfo=self.__tzinfo)
        else:
            raise TypeError("Cannot handle this type: ", type(other))
            
    def __add__(self, nanoseconds):
        """
        Return a new NanoTime object with the amount of nanoseconds added
        """
        self.__checkinit()
        if isinstance(nanoseconds, Nanoseconds):
            return NanoTime(self.__utc_value + nanoseconds.nanoseconds, tzinfo=self.__tzinfo)
        else:
            raise TypeError("Cannot handle this type: ", type(nanoseconds))

        
if __name__ == "__main__":
    import unittest
    unittest.main()
      
    class TestNanoTime(unittest.TestCase):
        def setUp(self):
            self.na = NanoTime()
            self.a = NanoTime(1543838936220310003)
            self.b = NanoTime(1543838936220310003, tzinfo=r"Australia/Sydney")
            self.c = NanoTime(1543838936220310002, tzinfo=r"Australia/Sydney")
            self.d = NanoTime(1543938936220310003)
              
        def test_initfail(self):
            prob = NanoTime()
            # Float init
            with self.assertRaises(ValueError):
                prob.initialize(154343700.1020175280, tzinfo=r"Australia/Sydney")
            # Negative init
            with self.assertRaises(ValueError):
                prob.initialize(-1, tzinfo=r"Australia/Sydney")
            # Tinezone fail
            with self.assertRaises(pytz.exceptions.UnknownTimeZoneError):
                prob.initialize(1543437001020175280, tzinfo=r"Australia\Sydney")
            self.assertEqual(str(prob), '')
            # Eventually works
            prob.initialize(1543437001020175280, tzinfo=r"Australia/Sydney")
            self.assertNotEqual(str(prob), '')
            
        def test_datetime(self):
            # No zone
            date_nozone = datetime(2018, 12, 3, 12, 8, 56, 220310)
            z = NanoTime(dt=date_nozone)
            # UTC
            date = datetime(2018, 12, 3, 12, 8, 56, 220310, tzinfo=pytz.utc)
            n = NanoTime(dt=date)
            # Australia
            date_syd = pytz.timezone("Australia/Sydney").localize(datetime(2018, 12, 3, 23, 8, 56, 220310))
            s = NanoTime(dt=date_syd)
            # Comparisons
            self.assertEqual(n, s)
            self.assertEqual(z, n)
            val = NanoTime(1543968001005589775)
            self.assertEqual(str(val), '2018-12-05T00:00:01.005589775')
        
        def test_addition(self):
            with self.assertRaises(TypeError):
                self.assertEqual(self.a+self.b, 1)
            self.assertEqual(self.c+Nanoseconds(1), self.b)
                         
        def test_subtraction(self):
            self.assertEqual((self.a - self.c).nanoseconds, 1)
            nanos = self.d - self.b
            self.assertEqual(nanos.nanoseconds, 100000000000000)
            self.assertEqual(nanos.microseconds, 100000000000.0)
            self.assertEqual(nanos.milliseconds, 100000000.0)
            self.assertEqual(nanos.seconds, 100000.0)
            self.assertAlmostEqual(nanos.minutes, 100000/60)
            self.assertAlmostEqual(nanos.hours, 100000/3600)
            self.assertEqual((self.d-self.c).nanoseconds, 100000000000001)
            
        def test_multiply(self):
            with self.assertRaises(TypeError):
                self.assertEqual(self.a * self.b, 1)
                
        def test_divide(self):
            with self.assertRaises(TypeError):
                self.assertEqual(self.a / self.b, 1)
                
        def test_mod(self):
            with self.assertRaises(TypeError):
                self.assertEqual(self.a ** self.b, 1)
            
        def test_len(self):
            self.assertEqual(0, len(self.na))
            self.assertEqual(1, len(self.a))
            
        def test_utc_value(self):
            self.assertEqual(str(self.a), '2018-12-03T12:08:56.220310003')
            
        def test_syd_value(self):
            self.assertEqual(str(self.b), '2018-12-03T23:08:56.220310003+11:00')
   
        def test_type_errors(self):
            with self.assertRaises(TypeError):
                self.assertEqual(self.a-1, 1)
            with self.assertRaises(TypeError):
                self.assertFalse(self.a < 1)
            with self.assertRaises(TypeError):
                self.assertFalse(self.a > 1)
            with self.assertRaises(TypeError):
                self.assertFalse(self.a <= 1)
            with self.assertRaises(TypeError):
                self.assertFalse(self.a >= 1)
                
        def test_unitialized(self):
            with self.assertRaises(ValueError):
                self.assertFalse(self.na < self.a)
            with self.assertRaises(ValueError):
                self.assertFalse(self.na > self.a)
            with self.assertRaises(ValueError):
                self.assertFalse(self.na == self.a)
            with self.assertRaises(ValueError):
                self.assertFalse(self.na - self.a == 0)
            
        def test_lessthan(self):
            self.assertTrue(self.a < self.d)
            self.assertTrue(self.b < self.d)
            self.assertTrue(self.c < self.d)
            self.assertFalse(self.a < self.b)
            self.assertTrue(self.c < self.a)
            self.assertTrue(self.c < self.b)
            
        def test_greaterthan(self):
            self.assertTrue(self.d > self.a)
            self.assertTrue(self.d > self.b)
            self.assertTrue(self.d > self.c)
            self.assertFalse(self.a > self.b)
            self.assertTrue(self.a > self.c)
            self.assertTrue(self.b > self.c)
            
        def test_equality(self):
            self.assertTrue(self.a == self.b)
            self.assertFalse(self.a == self.c)
            self.assertFalse(self.a == self.d)
            self.assertFalse(self.b == self.c)
            self.assertFalse(self.a == 1)
            self.assertTrue(self.a == 1543838936220310003)

        def test_nanos(self):
            self.assertEqual(Nanoseconds(1), Nanoseconds(1))
            self.assertNotEqual(Nanoseconds(1), Nanoseconds(10))
            self.assertEqual(Nanoseconds(1) + 1, Nanoseconds(2))
            self.assertEqual(Nanoseconds(2) - 1, Nanoseconds(1))
            self.assertTrue(Nanoseconds(2) > Nanoseconds(1))
            self.assertTrue(Nanoseconds(2) < Nanoseconds(3))
            self.assertFalse(Nanoseconds(2) < Nanoseconds(1))
            self.assertFalse(Nanoseconds(2) > Nanoseconds(3))
            self.assertTrue(Nanoseconds(2) <= Nanoseconds(3))
            self.assertTrue(Nanoseconds(2) <= Nanoseconds(2))
            self.assertTrue(Nanoseconds(3) >= Nanoseconds(3))
            self.assertTrue(Nanoseconds(3) >= Nanoseconds(2))
