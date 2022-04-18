/*
 * SPDX-FileCopyrightText: 2022 The HedgeDoc developers (see AUTHORS file)
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */
import { Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { TypeormStore } from 'connect-typeorm';
import { Repository } from 'typeorm';

import { DatabaseDialect } from '../config/database-dialect.enum';
import databaseConfiguration, {
  DatabaseConfig,
} from '../config/database.config';
import { Session } from '../users/session.entity';

interface SessionState {
  cookie: unknown;
  user: string;
  authProvider: string;
}

@Injectable()
export class SessionService {
  private readonly typeormStore: TypeormStore;

  constructor(
    @InjectRepository(Session) private sessionRepository: Repository<Session>,
    @Inject(databaseConfiguration.KEY)
    private dbConfig: DatabaseConfig,
  ) {
    this.typeormStore = new TypeormStore({
      cleanupLimit: 2,
      limitSubquery: dbConfig.dialect !== DatabaseDialect.MARIADB,
    }).connect(sessionRepository);
  }

  getTypeormStore(): TypeormStore {
    return this.typeormStore;
  }

  getUsernameFromSessionId(sessionId: string): Promise<string> {
    return new Promise((resolve, reject) => {
      this.typeormStore.get(sessionId, (error, result) => {
        if (error || !result) {
          return reject(error);
        }
        return resolve((result as SessionState).user);
      });
    });
  }
}
