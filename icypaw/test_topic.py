# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

from unittest import TestCase, main

from icypaw.topic import DeviceTopic


class TopicTest(TestCase):
    def test_match(self):
        dt = DeviceTopic("spBv1.0", "+", "DBIRTH", "+", "+")
        self.assertTrue(dt.match("spBv1.0/qscout/DBIRTH/plogger/DLpro_029201"))


if __name__ == "__main__":
    main()
