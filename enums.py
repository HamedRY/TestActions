from enum import Enum

class UserStatus(Enum):
    approve = 'approved'
    pending = 'pending'
    blocked = 'blocked'
    denied = 'denied'
    unknown = 'unknown'
    
class SubscriptionStatus(Enum):
    active = 'active'
    pending = 'pending'
    cancelled = 'cancelled'
    closed = 'closed'
    unknown = 'unknown'

class OrderStatus(Enum):
    approved = 'approved'
    pending = 'pending'
    cancel = 'cancel'
    unknown = 'unknown'

class Mode(Enum):
    development = 'development'
    stage = 'stage'
    production = 'production'
    
class TransactionType(Enum):
    internal = 'internal'
    external = 'external'

class Role(Enum):
    admin = 'ADMIN'
    support = 'SUPPORT'
    other = 'OTHER'

class LoginStates(Enum):
    success = 'success'
    active_2fa = 'active_2fa'
    retry_fail = 'retry_fail'
    fail = 'fail'

class CustomerType(Enum):
    all = 'all'
    kyc = 'kyc'
    nonkyc = 'nkyc'

class CustomerGender(Enum):
    all = 'all'
    male = 'male'
    female = 'female'

class GoldType(Enum):
    current = 'current'
    new = 'new'
    lost = 'lost'
    
class TimelineSteps(Enum):
    user_creation = {'operation': 'USER CREATED', 'description': 'user created with id'}
    bot_subscription = {'operation': 'BOT START', 'description': 'user started bot (name/amount)'}
    bot_unsubscription = {'operation': 'BOT STOP', 'description': 'user stoped bot'}
    deposit = {'operation': 'DEPOSIT', 'description': 'user deposited amount'}
    withdrawal = {'operation': 'WITHDRAWAL', 'description': 'user withdrawn amount' }

class DepositStage(Enum):
    all = 'all'
    first = 'first'