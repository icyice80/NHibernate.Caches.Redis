﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace NHibernate.Caches.Redis.Tests
{
    public class PerformanceTests : IntegrationTestBase
    {
        [Fact]
        async Task concurrent_reads_and_writes()
        {
            DisableLogging();

            const int iterations = 1000;
            var sessionFactory = CreateSessionFactory();

            var tasks = Enumerable.Range(0, iterations).Select(i =>
            {
                return Task.Run(() =>
                {
                    object entityId = null;
                    UsingSession(sessionFactory, session =>
                    {
                        var entity = new Person("Foo", 1);
                        entityId = session.Save(entity);
                        session.Flush();
                        session.Clear();
                    
                        entity = session.Load<Person>(entityId);
                        entity.Name = Guid.NewGuid().ToString();
                        session.Flush();
                    });
                });
            });

            await Task.WhenAll(tasks);
        }
    }
}
