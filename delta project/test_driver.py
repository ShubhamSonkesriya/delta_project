#Start testing by running this file

import pytest

# Run pytest.
retcode = pytest.main()

# Fail the if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."