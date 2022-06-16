class Error(Exception):
    """Base class for other exceptions"""
    pass


class EmptyPollError(Error):
    """Raised when a poll dataframe is empty"""
    pass

class NegativeDapprovalError(Exception):
    """Raised when negative values are found in dapproval data"""
    pass