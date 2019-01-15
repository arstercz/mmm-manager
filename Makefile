
ifndef INSTALLDIR
INSTALLDIR = installvendorlib
endif

MODULEDIR = $(DESTDIR)$(shell eval "`perl -V:${INSTALLDIR}`"; echo "$$${INSTALLDIR}")/MMMM
BINDIR    = $(DESTDIR)/usr/local/bin
SBINDIR   = $(DESTDIR)/usr/local/sbin
LOGDIR    = $(DESTDIR)/var/log/mmm-manager
ETCDIR    = $(DESTDIR)/etc
CONFDIR   = $(ETCDIR)/mmm-manager

install_common:
		mkdir -p $(DESTDIR) $(MODULEDIR) $(BINDIR) $(SBINDIR) $(LOGDIR) $(ETCDIR) $(CONFDIR)
		cp -r lib/MMMM/* $(MODULEDIR)
		[ -f $(CONFDIR)/mmm.conf ] || cp etc/mmm.conf $(CONFDIR)
		[ -f $(CONFDIR)/mmm-agent.conf ] || cp etc/mmm-agent.conf $(CONFDIR)

install_monitor: install_common
		cp -f bin/mmm-monitor ${BINDIR}

install_identify: install_common
		cp -f bin/mmm-identify ${BINDIR}

install_agent: install_common
		cp -f bin/mmm-agent ${BINDIR}

install_tools: install_common
		cp -f bin/mmm-status ${BINDIR}
		cp -f bin/mmm-uniqsign ${BINDIR}

install: install_monitor install_identify install_tools
