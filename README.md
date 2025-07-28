# jobsy

A simple, lightweight job scheduling library for Python that makes it easy to run tasks at specific times or intervals.

## Installation

```bash
pip install jobsy
```

## Quick Start

```python
import jobsy

jobsy.every('30m').do(my_function)

jobsy.at('09:00').do(daily_task)

jobsy.loop()
```

## Usage Examples

### Basic Scheduling

```python
import jobsy

def hello():
    print("Hello, World!")

def backup_database():
    print("Backing up database...")

# Runs every 5 seconds
jobsy.every('5s').do(hello)

# Runs every 30 minutes
jobsy.every('30m').do(backup_database)

# Runs daily at midnight
jobsy.at('00:00').do(backup_database)

# Start the scheduler (runs forever)
jobsy.loop()
```

### Time Windows

Restrict job execution to specific time periods:

```python
# Run every 5 minutes, but only between 9 AM and 6 PM
jobsy.every('5m').between('09:00', '18:00').do(market_data_update)

# Run every hour during business hours
jobsy.every('1h').between('09:00', '17:00').do(business_task)
```

### Day-Specific Scheduling

```python
# Run only on weekdays
jobsy.every('1h').on('mon', 'tue', 'wed', 'thu', 'fri').do(weekday_task)

# Run only on weekends
jobsy.every('30m').on('sat', 'sun').do(weekend_task)

# You can also use numbers (0=Monday, 6=Sunday)
jobsy.every('2h').on(0, 1, 2, 3, 4).do(weekday_task)
```

### Job Naming and Parameters

```python
def send_email(recipient, subject):
    print(f"Sending email to {recipient}: {subject}")

# Pass parameters to jobs
jobsy.every('1d').do(send_email, 'admin@example.com', 'Daily Report')

# Give jobs custom names
jobsy.every('1h').do(send_email, 'user@example.com', 'Hourly Update').as_name('hourly_email')
```

### Advanced Scheduling

```python
import jobsy

# Set timezone for accurate scheduling
jobsy.set_timezone('America/Sao_Paulo')

# Complex scheduling example
jobsy.every('5m').between('09:00', '18:00').on('mon', 'tue', 'wed', 'thu', 'fri').do(market_update)
jobsy.every('6h').do(data_sync)
jobsy.at('00:00').do(daily_cleanup)
jobsy.at('12:00').on('mon').do(weekly_report)

# Manual control
jobsy.run_pending()  # Run pending jobs once
jobsy.run_all()      # Run all jobs immediately

# Start the main loop
jobsy.loop(interval=60)  # Loops every 60 seconds
```

### Custom Scheduler

```python
from jobsy import Scheduler

scheduler = Scheduler()

scheduler.every('10s').do(my_task)
scheduler.at('15:30').do(afternoon_task)

scheduler.loop()
```

## API Reference

### Scheduling Methods

- `jobsy.every(interval)` - Schedule a periodic job
- `jobsy.at(time)` - Schedule a job at a specific time

### Interval Formats

- `'Xs'` - Every X seconds
- `'Xm'` - Every X minutes  
- `'Xh'` - Every X hours
- `'Xd'` - Every X days

### Time Formats

- `'HH:MM'` - 24-hour format (e.g., '09:30', '14:15')
- `'HH:MM:SS'` - With seconds (e.g., '09:30:00')

### Job Methods

- `.do(function, *args, **kwargs)` - Set the function to execute
- `.between(start, end)` - Restrict execution to time window
- `.on(*days)` - Run only on specific days
- `.as_name(name)` - Set a custom name for the job

### Day Names

- String format: `'mon'`, `'tue'`, `'wed'`, `'thu'`, `'fri'`, `'sat'`, `'sun'`
- Integer format: `0` (Monday) through `6` (Sunday)

### Control Methods

- `jobsy.loop(interval=1)` - Start the scheduler loop
- `jobsy.run_pending()` - Run pending jobs once
- `jobsy.run_all()` - Run all jobs immediately
- `jobsy.set_timezone(tz)` - Set timezone for scheduling

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.