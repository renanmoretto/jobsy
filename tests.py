import unittest
from unittest.mock import patch
from datetime import datetime, timedelta
import pytz

import jobsy
from jobsy import Scheduler, Job, _parse_at_time, _interval_to_seconds


class TestUtilityFunctions(unittest.TestCase):
    
    def test_parse_at_time(self):
        self.assertEqual(_parse_at_time('09:30'), (9, 30, 0))
        self.assertEqual(_parse_at_time('14:45:30'), (14, 45, 30))
    
    def test_interval_to_seconds(self):
        self.assertEqual(_interval_to_seconds('30s'), 30)
        self.assertEqual(_interval_to_seconds('5m'), 300)
        self.assertEqual(_interval_to_seconds('2h'), 7200)
        self.assertEqual(_interval_to_seconds('1d'), 86400)


class TestJob(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = Scheduler()
    
    def test_job_creation(self):
        job = Job(self.scheduler, job_type='periodic', interval='5m')
        self.assertEqual(job.interval, '5m')
        self.assertEqual(job.job_type, 'periodic')
        
        job = Job(self.scheduler, job_type='time', at='14:30')
        self.assertEqual(job.at, '14:30')
        self.assertEqual(job.job_type, 'time')
    
    def test_job_do_method(self):
        def test_func():
            return "test"
        
        job = Job(self.scheduler, job_type='periodic', interval='5m')
        result = job.do(test_func)
        
        self.assertIs(result, job)
        self.assertEqual(job.func(), "test")
    
    def test_job_do_with_parameters(self):
        """Test job execution with positional and keyword arguments"""
        executed_args = []
        executed_kwargs = {}
        
        def test_func_with_params(arg1, arg2, kwarg1=None, kwarg2=None):
            executed_args.extend([arg1, arg2])
            executed_kwargs.update({'kwarg1': kwarg1, 'kwarg2': kwarg2})
            return "executed"
        
        job = Job(self.scheduler, job_type='periodic', interval='5m')
        job.do(test_func_with_params, 'value1', 'value2', kwarg1='kw1', kwarg2='kw2')
        
        # Execute the job
        result = job.func()
        
        self.assertEqual(result, "executed")
        self.assertEqual(executed_args, ['value1', 'value2'])
        self.assertEqual(executed_kwargs, {'kwarg1': 'kw1', 'kwarg2': 'kw2'})
    
    def test_job_as_name(self):
        job = Job(self.scheduler, job_type='periodic', interval='5m')
        result = job.as_name('test_name')
        
        self.assertIs(result, job)
        self.assertEqual(job.name, 'test_name')
    
    def test_job_between(self):
        job = Job(self.scheduler, job_type='periodic', interval='30m')
        result = job.between('09:00', '17:00')
        
        self.assertIs(result, job)
        self.assertEqual(job.start, '09:00')
        self.assertEqual(job.end, '17:00')
    
    def test_job_on(self):
        job = Job(self.scheduler, job_type='periodic', interval='1h')
        result = job.on('mon', 'wed', 'fri')
        
        self.assertIs(result, job)
        self.assertEqual(job.days_int, [0, 2, 4])


class TestScheduler(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = Scheduler()
    
    def test_scheduler_every(self):
        job = self.scheduler.every('5m')
        
        self.assertIsInstance(job, Job)
        self.assertEqual(job.interval, '5m')
        self.assertIn(job, self.scheduler.jobs)
    
    def test_scheduler_at(self):
        job = self.scheduler.at('14:30')
        
        self.assertIsInstance(job, Job)
        self.assertEqual(job.at, '14:30')
        self.assertIn(job, self.scheduler.jobs)
    
    def test_run_pending(self):
        executed = []
        def test_func():
            executed.append('called')
        
        job = self.scheduler.every('5m').do(test_func)
        job.next_run = datetime.now() - timedelta(minutes=1)  # Make it ready to run
        
        self.scheduler.run_pending()
        self.assertEqual(len(executed), 1)
    
    def test_run_all(self):
        executed = []
        
        def test_func1():
            executed.append('func1')
        
        def test_func2():
            executed.append('func2')
        
        self.scheduler.every('5m').do(test_func1)
        self.scheduler.at('14:30').do(test_func2)
        
        self.scheduler.run_all()
        
        self.assertIn('func1', executed)
        self.assertIn('func2', executed)


class TestTimeCalculations(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = Scheduler()
    
    def test_next_run_is_set(self):
        job = Job(self.scheduler, job_type='periodic', interval='30m')
        self.assertIsNotNone(job.next_run)
        
        job = Job(self.scheduler, job_type='time', at='14:30')
        self.assertIsNotNone(job.next_run)
    
    def test_should_run_true(self):
        job = Job(self.scheduler, job_type='periodic', interval='30m')
        job.next_run = datetime.now() - timedelta(minutes=1)
        
        self.assertTrue(job.should_run)
    
    def test_should_run_false(self):
        job = Job(self.scheduler, job_type='periodic', interval='30m')
        job.next_run = datetime.now() + timedelta(minutes=30)
        
        self.assertFalse(job.should_run)


class TestTimeWindows(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = Scheduler()
    
    def test_time_window_restrictions(self):
        """Test that jobs respect time window restrictions"""
        job = Job(self.scheduler, job_type='periodic', interval='1h')
        job.between('09:00', '17:00')
        
        # Mock current time to 10:00 AM
        mock_now = datetime(2024, 1, 1, 10, 0, 0)
        
        with patch.object(job, '_now', return_value=mock_now):
            # Set last_run to trigger calculation
            job.last_run = mock_now - timedelta(hours=1)
            
            # Should schedule next run within the time window
            next_run = job._calc_next_run()
            self.assertTrue(next_run.hour >= 9)
            self.assertTrue(next_run.hour <= 17)
    
    def test_time_window_with_day_restrictions(self):
        """Test that jobs move to next day when outside time window with day restrictions"""
        job = Job(self.scheduler, job_type='periodic', interval='1h')
        job.between('09:00', '17:00').on('mon', 'tue', 'wed', 'thu', 'fri')
        
        # Mock current time to Monday 6:00 PM (18:00)
        mock_now = datetime(2024, 1, 1, 18, 0, 0)  # Monday
        
        with patch.object(job, '_now', return_value=mock_now):
            job.last_run = mock_now - timedelta(hours=1)
            
            next_run = job._calc_next_run()
            
            # Should be scheduled for next day (Tuesday) at start time
            self.assertEqual(next_run.date(), (mock_now + timedelta(days=1)).date())
            self.assertEqual(next_run.hour, 9)
    
    def test_time_window_without_day_restrictions_current_behavior(self):
        """Test current behavior: time windows without day restrictions ignore time window logic"""
        job = Job(self.scheduler, job_type='periodic', interval='1h')
        job.between('09:00', '17:00')  # No day restrictions
        
        # Mock current time to 6:00 PM (18:00) - outside window
        mock_now = datetime(2024, 1, 1, 18, 0, 0)
        
        with patch.object(job, '_now', return_value=mock_now):
            job.last_run = mock_now - timedelta(hours=1)  # 17:00
            
            next_run = job._calc_next_run()
            
            # Current implementation: ignores time window when no day restrictions
            # Simply adds interval to last_run (17:00 + 1h = 18:00)
            self.assertEqual(next_run.hour, 18)
            self.assertEqual(next_run.date(), mock_now.date())


class TestDaySpecificScheduling(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = Scheduler()
    
    def test_weekday_scheduling(self):
        """Test scheduling jobs only on weekdays"""
        job = Job(self.scheduler, job_type='time', at='14:30')
        job.on('mon', 'tue', 'wed', 'thu', 'fri')  # Weekdays only
        
        # Mock current time to Saturday (weekday 5)
        mock_now = datetime(2024, 1, 6, 10, 0, 0)  # Saturday
        
        with patch.object(job, '_now', return_value=mock_now):
            next_run = job._calc_next_run()
            
            # Should be scheduled for next Monday (weekday 0)
            self.assertEqual(next_run.weekday(), 0)  # Monday
            self.assertEqual(next_run.hour, 14)
            self.assertEqual(next_run.minute, 30)
    
    def test_weekend_scheduling(self):
        """Test scheduling jobs only on weekends"""
        job = Job(self.scheduler, job_type='time', at='14:30')
        job.on('sat', 'sun')  # Weekends only
        
        # Mock current time to Wednesday (weekday 2)
        mock_now = datetime(2024, 1, 3, 10, 0, 0)  # Wednesday
        
        with patch.object(job, '_now', return_value=mock_now):
            next_run = job._calc_next_run()
            
            # Should be scheduled for next Saturday (weekday 5)
            self.assertEqual(next_run.weekday(), 5)  # Saturday
            self.assertEqual(next_run.hour, 14)
            self.assertEqual(next_run.minute, 30)
    
    def test_numeric_day_scheduling(self):
        """Test scheduling with numeric day format"""
        job = Job(self.scheduler, job_type='time', at='14:30')
        job.on(0, 2, 4)  # Monday, Wednesday, Friday
        
        # Mock current time to Thursday (weekday 3)
        mock_now = datetime(2024, 1, 4, 15, 0, 0)  # Thursday
        
        with patch.object(job, '_now', return_value=mock_now):
            next_run = job._calc_next_run()
            
            # Should be scheduled for next Friday (weekday 4)
            self.assertEqual(next_run.weekday(), 4)  # Friday
            self.assertEqual(next_run.hour, 14)
            self.assertEqual(next_run.minute, 30)
    
    def test_invalid_day_string(self):
        """Test that invalid day strings raise ValueError"""
        job = Job(self.scheduler, job_type='periodic', interval='1h')
        
        with self.assertRaises(ValueError):
            job.on('invalid_day')
    
    def test_invalid_day_number(self):
        """Test that invalid day numbers raise ValueError"""
        job = Job(self.scheduler, job_type='periodic', interval='1h')
        
        with self.assertRaises(ValueError):
            job.on(7)  # Valid range is 0-6
        
        with self.assertRaises(ValueError):
            job.on(-1)  # Negative numbers are invalid


class TestSafeExecution(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = Scheduler()
    
    @patch('builtins.print')
    @patch('traceback.print_exc')
    def test_safe_execution_catches_exceptions(self, mock_print_exc, mock_print):
        """Test that safe execution catches and handles exceptions"""
        def failing_job():
            raise Exception("Test exception")
        
        # Test safe execution (default)
        scheduler = Scheduler(safe_execution=True)
        job = Job(scheduler, job_type='periodic', interval='5m')
        job.do(failing_job)
        job.as_name('test_job')
        
        # Should not raise exception
        job.run(safe=True)
        
        # Should print error message
        mock_print.assert_called_with('Error executing job test_job, Test exception')
        mock_print_exc.assert_called_once()
    
    def test_unsafe_execution_propagates_exceptions(self):
        """Test that unsafe execution propagates exceptions"""
        def failing_job():
            raise ValueError("Test exception")
        
        scheduler = Scheduler(safe_execution=False)
        job = Job(scheduler, job_type='periodic', interval='5m')
        job.do(failing_job)
        
        # Should raise the original exception
        with self.assertRaises(ValueError) as context:
            job.run(safe=False)
        
        self.assertEqual(str(context.exception), "Test exception")
    
    @patch('builtins.print')
    @patch('traceback.print_exc')
    def test_safe_wrap_function_name(self, mock_print_exc, mock_print):
        """Test that _safe_wrap handles function names correctly"""
        from jobsy import _safe_wrap
        
        def named_function():
            raise Exception("Test error")
        
        # Test with named function
        safe_func = _safe_wrap(named_function)
        safe_func()
        
        mock_print.assert_called_with('Error executing job named_function, Test error')
        mock_print_exc.assert_called_once()
    
    @patch('builtins.print')
    @patch('traceback.print_exc')
    def test_safe_wrap_custom_name(self, mock_print_exc, mock_print):
        """Test that _safe_wrap uses custom job name when provided"""
        from jobsy import _safe_wrap
        
        def some_function():
            raise Exception("Test error")
        
        safe_func = _safe_wrap(some_function, 'custom_job_name')
        safe_func()
        
        mock_print.assert_called_with('Error executing job custom_job_name, Test error')
        mock_print_exc.assert_called_once()
    
    def test_scheduler_safe_execution_setting(self):
        """Test that scheduler safe_execution setting is used correctly"""
        executed = []
        
        def test_func():
            executed.append('called')
        
        # Safe scheduler
        safe_scheduler = Scheduler(safe_execution=True)
        job1 = safe_scheduler.every('5m').do(test_func)
        job1.next_run = datetime.now() - timedelta(minutes=1)
        
        safe_scheduler.run_pending()
        self.assertEqual(len(executed), 1)
        
        # Unsafe scheduler
        unsafe_scheduler = Scheduler(safe_execution=False)
        job2 = unsafe_scheduler.every('5m').do(test_func)
        job2.next_run = datetime.now() - timedelta(minutes=1)
        
        unsafe_scheduler.run_pending()
        self.assertEqual(len(executed), 2)


class TestTimezoneSupport(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = Scheduler()
    
    def test_set_timezone_updates_scheduler(self):
        """Test that setting timezone updates the scheduler and all jobs"""
        utc_tz = pytz.timezone('UTC')
        sp_tz = pytz.timezone('America/Sao_Paulo')
        
        # Add jobs before setting timezone
        job1 = self.scheduler.every('1h')
        job2 = self.scheduler.at('14:30')
        
        # Set timezone
        self.scheduler.set_timezone('America/Sao_Paulo')
        
        # Check scheduler timezone
        self.assertEqual(str(self.scheduler.tz), 'America/Sao_Paulo')
        
        # Check that all jobs got the timezone
        for job in self.scheduler.jobs:
            self.assertEqual(str(job.tz), 'America/Sao_Paulo')
    
    def test_job_timezone_aware_scheduling(self):
        """Test that jobs respect timezone for scheduling"""
        # Set timezone to a specific one
        self.scheduler.set_timezone('America/New_York')
        ny_tz = pytz.timezone('America/New_York')
        
        job = self.scheduler.at('14:30')
        
        # Check that next_run uses the correct timezone
        self.assertEqual(job.next_run.tzinfo, ny_tz)
    
    def test_timezone_conversion(self):
        """Test timezone-aware time calculations"""
        # Set scheduler to São Paulo timezone
        self.scheduler.set_timezone('America/Sao_Paulo')
        
        # Create job after setting timezone so it gets the timezone
        job = self.scheduler.at('14:30')
        
        # Mock _now to return a specific time in São Paulo
        sp_tz = pytz.timezone('America/Sao_Paulo')
        mock_now = sp_tz.localize(datetime(2024, 1, 1, 10, 0, 0))
        
        with patch.object(job, '_now', return_value=mock_now):
            next_run = job._calc_next_run()
            
            # Should be in São Paulo timezone
            self.assertEqual(next_run.tzinfo, sp_tz)
            self.assertEqual(next_run.hour, 14)
            self.assertEqual(next_run.minute, 30)
    
    def test_module_level_set_timezone(self):
        """Test module-level set_timezone function"""
        # Test that module function works
        jobsy.set_timezone('UTC')
        self.assertEqual(str(jobsy.default_scheduler.tz), 'UTC')
        
        # Test that new jobs get the timezone
        job = jobsy.every('1h')
        self.assertEqual(str(job.tz), 'UTC')
    
    def test_naive_datetime_handling(self):
        """Test that jobs work without timezone (naive datetime)"""
        # Don't set any timezone
        job = Job(self.scheduler, job_type='time', at='14:30')
        
        # Should work with None timezone
        self.assertIsNone(job.tz)
        self.assertIsNotNone(job.next_run)


class TestComplexScheduling(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = Scheduler()
    
    def test_combined_time_window_and_days(self):
        """Test jobs with both time windows and day restrictions"""
        # Create job that runs hourly, weekdays only, between 9-5
        job = Job(self.scheduler, job_type='periodic', interval='1h')
        job.between('09:00', '17:00').on('mon', 'tue', 'wed', 'thu', 'fri')
        
        # Mock current time to Saturday at 10:00 AM
        mock_now = datetime(2024, 1, 6, 10, 0, 0)  # Saturday
        
        with patch.object(job, '_now', return_value=mock_now):
            job.last_run = mock_now - timedelta(hours=1)
            
            next_run = job._calc_next_run()
            
            # Should be scheduled for next Monday at 9:00 AM
            self.assertEqual(next_run.weekday(), 0)  # Monday
            self.assertEqual(next_run.hour, 9)
            self.assertEqual(next_run.minute, 0)
    
    def test_time_job_with_day_restrictions(self):
        """Test at() jobs with day restrictions"""
        # Job runs at 2:30 PM but only on Mondays
        job = Job(self.scheduler, job_type='time', at='14:30')
        job.on('mon')
        
        # Mock current time to Friday at 10:00 AM
        mock_now = datetime(2024, 1, 5, 10, 0, 0)  # Friday
        
        with patch.object(job, '_now', return_value=mock_now):
            next_run = job._calc_next_run()
            
            # Should be scheduled for next Monday at 2:30 PM
            self.assertEqual(next_run.weekday(), 0)  # Monday
            self.assertEqual(next_run.hour, 14)
            self.assertEqual(next_run.minute, 30)
    
    def test_chained_method_calls(self):
        """Test that method chaining works correctly"""
        executed = []
        
        def test_func(msg):
            executed.append(msg)
        
        # Test chaining all methods
        job = (self.scheduler.every('1h')
               .between('09:00', '17:00')
               .on('mon', 'tue', 'wed', 'thu', 'fri')
               .do(test_func, 'chained call')
               .as_name('complex_job'))
        
        # Verify all properties are set
        self.assertEqual(job.start, '09:00')
        self.assertEqual(job.end, '17:00')
        self.assertEqual(job.days_int, [0, 1, 2, 3, 4])
        self.assertEqual(job.name, 'complex_job')
        
        # Test execution
        job.func()
        self.assertEqual(executed, ['chained call'])


class TestAdditionalModuleFunctions(unittest.TestCase):
    
    def test_module_loop_function(self):
        """Test module-level loop function with custom scheduler"""
        executed = []
        
        def test_func():
            executed.append('called')
        
        custom_scheduler = Scheduler()
        job = custom_scheduler.every('1h').do(test_func)
        job.next_run = datetime.now() - timedelta(minutes=1)
        
        # Mock time.sleep to avoid infinite loop
        with patch('time.sleep', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                jobsy.loop(interval=1, scheduler=custom_scheduler)
        
        # Should have executed the pending job
        self.assertEqual(len(executed), 1)
    
    def test_module_run_pending_with_custom_scheduler(self):
        """Test module-level run_pending with custom scheduler"""
        executed = []
        
        def test_func():
            executed.append('called')
        
        custom_scheduler = Scheduler()
        job = custom_scheduler.every('1h').do(test_func)
        job.next_run = datetime.now() - timedelta(minutes=1)
        
        jobsy.run_pending(scheduler=custom_scheduler)
        self.assertEqual(len(executed), 1)
    
    def test_module_run_all_with_custom_scheduler(self):
        """Test module-level run_all with custom scheduler"""
        executed = []
        
        def test_func1():
            executed.append('func1')
        
        def test_func2():
            executed.append('func2')
        
        custom_scheduler = Scheduler()
        custom_scheduler.every('1h').do(test_func1)
        custom_scheduler.at('14:30').do(test_func2)
        
        jobsy.run_all(scheduler=custom_scheduler)
        
        self.assertIn('func1', executed)
        self.assertIn('func2', executed)
        self.assertEqual(len(executed), 2)


class TestModuleFunctions(unittest.TestCase):
    
    def test_every_function(self):
        job = jobsy.every('5m')
        self.assertIsInstance(job, Job)
        self.assertEqual(job.interval, '5m')
    
    def test_at_function(self):
        job = jobsy.at('14:30')
        self.assertIsInstance(job, Job)
        self.assertEqual(job.at, '14:30')
    
    def test_set_timezone(self):
        jobsy.set_timezone('UTC')
        self.assertEqual(str(jobsy.default_scheduler.tz), 'UTC')


if __name__ == '__main__':
    unittest.main() 