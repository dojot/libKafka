#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([libadminkafka], [0.2.0], [muhammed@cpqd.com.br])
AC_CONFIG_SRCDIR([src/])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE([-Wall -Werror foreign])
# Checks for programs.
AC_PROG_CXX

CXXFLAGS+=" -Wall -Werror"

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AM_PROG_AR
AC_PROG_RANLIB
PKG_INSTALLDIR

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 libadminkafka.pc:libadminkafka.pc.in])
AC_OUTPUT
