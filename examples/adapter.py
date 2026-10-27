"""A runnable adapter factory using the isolated reference receiver."""

from hookcontract.reference import ManagedTarget


def target():
    return ManagedTarget()
